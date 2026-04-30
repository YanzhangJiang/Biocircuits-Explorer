const BEHAVIOR_PROGRAM_CODEC_VERSION = 1
const BEHAVIOR_PROGRAM_MAGIC = UInt8['R', 'P', 'B', '1']
const DEFAULT_RO_QUANTIZATION_DIGITS = 3
const DEFAULT_RO_QUANTIZATION_SCALE = 1000
const DEFAULT_PROGRAM_IDENTITY = "exact_profile_v1"
const DEFAULT_SUPPORT_SEMANTICS = "included_exact_family_path_count_v1"

function behavior_program_config_from_raw(raw=nothing)
    if raw isa AtlasBehaviorConfig
        raw = atlas_behavior_config_to_dict(raw)
    end
    cfg = raw isa AbstractDict ? raw : Dict{String, Any}()
    digits = Int(_raw_get(cfg, :ro_quantization_digits, DEFAULT_RO_QUANTIZATION_DIGITS))
    scale = Int(_raw_get(cfg, :ro_quantization_scale, DEFAULT_RO_QUANTIZATION_SCALE))
    return Dict{String, Any}(
        "path_scope" => String(_raw_get(cfg, :path_scope, "feasible")),
        "min_volume_mean" => Float64(_raw_get(cfg, :min_volume_mean, 0.0)),
        "deduplicate" => Bool(_raw_get(cfg, :deduplicate, true)),
        "keep_singular" => Bool(_raw_get(cfg, :keep_singular, true)),
        "keep_nonasymptotic" => Bool(_raw_get(cfg, :keep_nonasymptotic, false)),
        "compute_volume" => Bool(_raw_get(cfg, :compute_volume, false)),
        "motif_zero_tol" => Float64(_raw_get(cfg, :motif_zero_tol, 1e-6)),
        "ro_quantization_digits" => digits,
        "ro_quantization_scale" => scale,
        "program_identity" => String(_raw_get(cfg, :program_identity, DEFAULT_PROGRAM_IDENTITY)),
        "support_semantics" => String(_raw_get(cfg, :support_semantics, DEFAULT_SUPPORT_SEMANTICS)),
    )
end

behavior_program_quantization_scale(cfg) =
    Int(_raw_get(behavior_program_config_from_raw(cfg), :ro_quantization_scale, DEFAULT_RO_QUANTIZATION_SCALE))

behavior_program_quantization_digits(cfg) =
    Int(_raw_get(behavior_program_config_from_raw(cfg), :ro_quantization_digits, DEFAULT_RO_QUANTIZATION_DIGITS))

function _program_varuint_push!(buf::Vector{UInt8}, value::Integer)
    value >= 0 || error("varuint expects a non-negative integer")
    current = UInt64(value)
    while current >= 0x80
        push!(buf, UInt8((current & 0x7f) | 0x80))
        current >>= 7
    end
    push!(buf, UInt8(current))
    return buf
end

function _program_varuint_read(bytes::AbstractVector{UInt8}, pos::Int)
    shift = 0
    value = UInt64(0)
    while true
        pos <= length(bytes) || error("Unexpected end of behavior program blob")
        byte = bytes[pos]
        pos += 1
        value |= UInt64(byte & 0x7f) << shift
        (byte & 0x80) == 0 && return Int(value), pos
        shift += 7
        shift <= 63 || error("Behavior program varuint is too large")
    end
end

function _program_zigzag_push!(buf::Vector{UInt8}, value::Integer)
    signed = Int(value)
    encoded = signed >= 0 ? (UInt64(signed) << 1) : ((UInt64(-signed) << 1) - 1)
    return _program_varuint_push!(buf, encoded)
end

function _program_zigzag_read(bytes::AbstractVector{UInt8}, pos::Int)
    encoded, pos = _program_varuint_read(bytes, pos)
    unsigned = UInt64(encoded)
    value = (unsigned & 0x01) == 0 ? Int(unsigned >> 1) : -Int((unsigned + 1) >> 1)
    return value, pos
end

function _program_push_text!(buf::Vector{UInt8}, text::AbstractString)
    raw = collect(codeunits(String(text)))
    _program_varuint_push!(buf, length(raw))
    append!(buf, raw)
    return buf
end

function _program_read_text(bytes::AbstractVector{UInt8}, pos::Int)
    len, pos = _program_varuint_read(bytes, pos)
    stop = pos + len - 1
    stop <= length(bytes) || error("Unexpected end of behavior program text atom")
    return String(bytes[pos:stop]), stop + 1
end

function _program_token_from_scaled(value::Integer, scale::Integer)
    value == 0 && return "0"
    sign = value > 0 ? "+" : "-"
    abs_value = abs(Int(value))
    whole = abs_value ÷ scale
    frac = abs_value % scale
    if frac == 0
        return sign * string(whole)
    end
    width = max(1, length(string(scale - 1)))
    frac_text = rstrip(lpad(string(frac), width, '0'), '0')
    return sign * string(whole, ".", frac_text)
end

function _program_numeric_scaled_token(value::Real, cfg)
    val = Float64(value)
    isnan(val) && return "NaN"
    isinf(val) && return signbit(val) ? "-Inf" : "+Inf"
    scale = behavior_program_quantization_scale(cfg)
    scaled = round(Int, val * scale)
    return _program_token_from_scaled(scaled, scale)
end

function _program_numeric_scaled_token(text::AbstractString, cfg)
    stripped = strip(String(text))
    isempty(stripped) && return "missing"
    lowered = lowercase(stripped)
    lowered in ("nan", "+nan", "-nan") && return "NaN"
    lowered in ("inf", "+inf", "infinity", "+infinity") && return "+Inf"
    lowered in ("-inf", "-infinity") && return "-Inf"
    lowered in ("missing", "undef", "undefined", "nothing") && return "missing"
    parsed = tryparse(Float64, stripped)
    parsed === nothing && return stripped
    return _program_numeric_scaled_token(parsed, cfg)
end

_program_numeric_scaled_token(value, cfg) = _program_numeric_scaled_token(string(value), cfg)

function _program_split_vector_label(text::AbstractString)
    stripped = strip(String(text))
    if startswith(stripped, "[") && endswith(stripped, "]")
        inner = stripped[2:(end - 1)]
        return isempty(strip(inner)) ? String[] : [strip(part) for part in split(inner, ",")]
    elseif startswith(stripped, "(") && endswith(stripped, ")")
        inner = stripped[2:(end - 1)]
        return isempty(strip(inner)) ? String[] : [strip(part) for part in split(inner, ",")]
    end
    return nothing
end

function _program_canonical_state(state, cfg)
    if state isa AbstractString
        vector_parts = _program_split_vector_label(state)
        vector_parts === nothing || return [_program_numeric_scaled_token(part, cfg) for part in vector_parts]
        return [_program_numeric_scaled_token(state, cfg)]
    elseif state isa AbstractVector || state isa Tuple
        return [_program_numeric_scaled_token(coord, cfg) for coord in collect(state)]
    else
        return [_program_numeric_scaled_token(state, cfg)]
    end
end

function behavior_program_profile_from_label(label::AbstractString)
    text = strip(String(label))
    isempty(text) && return Any[]
    return Any[strip(part) for part in split(text, " -> ")]
end

function canonical_program_profile(profile, cfg=Dict{String, Any}())
    raw_profile = profile
    if raw_profile isa AbstractDict
        raw_profile = _raw_get(raw_profile, :exact_profile, _raw_get(raw_profile, :profile, Any[]))
        if (raw_profile === nothing || isempty(collect(raw_profile))) && _raw_haskey(profile, :exact_label)
            raw_profile = behavior_program_profile_from_label(String(_raw_get(profile, :exact_label, "")))
        end
    elseif raw_profile isa AbstractString
        raw_profile = behavior_program_profile_from_label(raw_profile)
    elseif raw_profile === nothing
        raw_profile = Any[]
    end

    canonical = Vector{Vector{String}}()
    for state in collect(raw_profile)
        push!(canonical, _program_canonical_state(state, cfg))
    end
    return canonical
end

function _program_token_code(token::AbstractString)
    text = strip(String(token))
    text == "NaN" && return UInt8(0x01), nothing
    text == "+Inf" && return UInt8(0x02), nothing
    text == "-Inf" && return UInt8(0x03), nothing
    text == "missing" && return UInt8(0x04), nothing
    parsed = tryparse(Float64, text)
    if parsed !== nothing && isfinite(parsed)
        return UInt8(0x00), parsed
    end
    return UInt8(0x05), text
end

function _program_encode_atom!(buf::Vector{UInt8}, token::AbstractString, cfg)
    tag, value = _program_token_code(token)
    push!(buf, tag)
    if tag == 0x00
        scale = behavior_program_quantization_scale(cfg)
        _program_zigzag_push!(buf, round(Int, Float64(value) * scale))
    elseif tag == 0x05
        _program_push_text!(buf, String(value))
    end
    return buf
end

function encode_program_blob(profile, cfg=Dict{String, Any}())
    normalized_cfg = behavior_program_config_from_raw(cfg)
    canonical = canonical_program_profile(profile, normalized_cfg)
    dim = isempty(canonical) ? 0 : length(first(canonical))
    for state in canonical
        length(state) == dim || error("Behavior program states must have a consistent dimension.")
    end

    buf = copy(BEHAVIOR_PROGRAM_MAGIC)
    _program_varuint_push!(buf, BEHAVIOR_PROGRAM_CODEC_VERSION)
    _program_varuint_push!(buf, length(canonical))
    _program_varuint_push!(buf, dim)
    for state in canonical
        for atom in state
            _program_encode_atom!(buf, atom, normalized_cfg)
        end
    end
    return buf
end

function decode_program_blob(blob, cfg=Dict{String, Any}())
    bytes = UInt8.(collect(blob))
    length(bytes) >= length(BEHAVIOR_PROGRAM_MAGIC) || error("Behavior program blob is too short")
    bytes[1:length(BEHAVIOR_PROGRAM_MAGIC)] == BEHAVIOR_PROGRAM_MAGIC || error("Behavior program blob has an invalid magic header")
    normalized_cfg = behavior_program_config_from_raw(cfg)
    pos = length(BEHAVIOR_PROGRAM_MAGIC) + 1
    version, pos = _program_varuint_read(bytes, pos)
    version == BEHAVIOR_PROGRAM_CODEC_VERSION || error("Unsupported behavior program codec version: $version")
    len, pos = _program_varuint_read(bytes, pos)
    dim, pos = _program_varuint_read(bytes, pos)
    scale = behavior_program_quantization_scale(normalized_cfg)

    out = Vector{Vector{String}}()
    for _ in 1:len
        state = String[]
        for _ in 1:dim
            pos <= length(bytes) || error("Unexpected end of behavior program atom")
            tag = bytes[pos]
            pos += 1
            token = if tag == 0x00
                scaled, next_pos = _program_zigzag_read(bytes, pos)
                pos = next_pos
                _program_token_from_scaled(scaled, scale)
            elseif tag == 0x01
                "NaN"
            elseif tag == 0x02
                "+Inf"
            elseif tag == 0x03
                "-Inf"
            elseif tag == 0x04
                "missing"
            elseif tag == 0x05
                text, next_pos = _program_read_text(bytes, pos)
                pos = next_pos
                text
            else
                error("Unknown behavior program atom tag: $tag")
            end
            push!(state, token)
        end
        push!(out, state)
    end
    pos == length(bytes) + 1 || error("Behavior program blob has trailing bytes")
    return out
end

behavior_program_hash(blob::AbstractVector{UInt8}) = bytes2hex(SHA.sha256(blob))

function program_exact_label(profile, cfg=Dict{String, Any}())
    canonical = canonical_program_profile(profile, cfg)
    isempty(canonical) && return ""
    dim = length(first(canonical))
    rendered = map(canonical) do state
        dim == 1 ? first(state) : "[" * join(state, ",") * "]"
    end
    return join(rendered, " -> ")
end

function _program_token_float(token::AbstractString)
    text = strip(String(token))
    text == "+Inf" && return Inf
    text == "-Inf" && return -Inf
    text == "NaN" && return NaN
    text == "missing" && return NaN
    parsed = tryparse(Float64, text)
    return parsed === nothing ? NaN : parsed
end

function _program_token_sign(token::AbstractString; zero_tol::Real=1e-6)
    value = _program_token_float(token)
    isnan(value) && return 9
    value > zero_tol && return 1
    value < -zero_tol && return -1
    return 0
end

function program_motif_label(profile, cfg=Dict{String, Any}(); motif_zero_tol=nothing)
    normalized_cfg = behavior_program_config_from_raw(cfg)
    zero_tol = motif_zero_tol === nothing ? Float64(_raw_get(normalized_cfg, :motif_zero_tol, 1e-6)) : Float64(motif_zero_tol)
    canonical = canonical_program_profile(profile, normalized_cfg)
    isempty(canonical) && return ""
    dim = length(first(canonical))
    rendered = map(canonical) do state
        signs = map(state) do atom
            sign_code = _program_token_sign(atom; zero_tol=zero_tol)
            sign_code > 0 ? "+" : sign_code < 0 ? "-" : sign_code == 0 ? "0" : "?"
        end
        dim == 1 ? first(signs) : "[" * join(signs, ",") * "]"
    end
    return join(rendered, " -> ")
end

function program_features(profile, cfg=Dict{String, Any}())
    normalized_cfg = behavior_program_config_from_raw(cfg)
    canonical = canonical_program_profile(profile, normalized_cfg)
    len = length(canonical)
    dim = isempty(canonical) ? 0 : length(first(canonical))
    distinct = length(unique([join(state, ",") for state in canonical]))
    zero_tol = Float64(_raw_get(normalized_cfg, :motif_zero_tol, 1e-6))

    sign_changes = 0
    total_variation = 0.0
    for idx in 1:max(0, len - 1)
        a = canonical[idx]
        b = canonical[idx + 1]
        signs_a = [_program_token_sign(token; zero_tol=zero_tol) for token in a]
        signs_b = [_program_token_sign(token; zero_tol=zero_tol) for token in b]
        sign_changes += signs_a == signs_b ? 0 : 1

        l1 = 0.0
        support_changed = false
        for j in 1:dim
            va = _program_token_float(a[j])
            vb = _program_token_float(b[j])
            if isfinite(va) && isfinite(vb)
                l1 += abs(va - vb)
            elseif !isequal(va, vb)
                l1 += 1.0
            end
            support_changed |= (_program_token_sign(a[j]; zero_tol=zero_tol) != 0) != (_program_token_sign(b[j]; zero_tol=zero_tol) != 0)
        end
        total_variation += (signs_a == signs_b ? 0.0 : 1.0) + l1 / max(1, dim) + (support_changed ? 1.0 : 0.0)
    end

    active_dim = 0
    for j in 1:dim
        values = [_program_token_float(state[j]) for state in canonical]
        finite_values = [value for value in values if isfinite(value)]
        if any(value -> abs(value) > zero_tol, finite_values) || length(unique(finite_values)) > 1
            active_dim += 1
        end
    end

    has_nan = any(state -> any(atom -> atom == "NaN" || atom == "missing", state), canonical)
    has_inf = any(state -> any(atom -> atom in ("+Inf", "-Inf"), state), canonical)
    singular_count = count(state -> any(atom -> atom in ("NaN", "+Inf", "-Inf", "missing"), state), canonical)

    return Dict{String, Any}(
        "c_len" => Float64(len),
        "c_distinct" => Float64(distinct),
        "c_sign_changes" => Float64(sign_changes),
        "c_total_variation" => total_variation,
        "c_active_dim" => Float64(active_dim),
        "c_singular" => Float64(singular_count),
        "len" => len,
        "dim" => dim,
        "has_singular" => singular_count > 0,
        "has_nan" => has_nan,
        "has_inf" => has_inf,
    )
end
