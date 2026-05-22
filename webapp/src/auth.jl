# Pure-Julia Cognito ID/Access token verification.
# No new package dependencies: uses Base64 + SHA + BigInt + HTTP/JSON3 (already
# loaded by the main module).

const _JWKS_CACHE = Dict{String, Dict{String, NamedTuple{(:n, :e), Tuple{BigInt, BigInt}}}}()
const _JWKS_CACHE_LOCK = ReentrantLock()

const _SHA256_DIGEST_INFO_PREFIX = UInt8[
    0x30, 0x31, 0x30, 0x0D, 0x06, 0x09, 0x60, 0x86, 0x48, 0x01,
    0x65, 0x03, 0x04, 0x02, 0x01, 0x05, 0x00, 0x04, 0x20,
]

# RFC 7515 base64url decoder. Cognito JWTs omit padding.
function _b64url_decode(s::AbstractString)
    s = String(s)
    pad = (4 - mod(length(s), 4)) % 4
    return base64decode(replace(s, '-' => '+', '_' => '/') * ("=" ^ pad))
end

function _b64url_decode_bigint(s::AbstractString)
    bytes = _b64url_decode(s)
    isempty(bytes) && return BigInt(0)
    return parse(BigInt, "0x" * bytes2hex(bytes))
end

# RSASSA-PKCS1-v1_5 verify with SHA-256. Returns Bool.
function _rsa_pkcs1_v15_verify_sha256(n::BigInt, e::BigInt, message::Vector{UInt8}, signature::Vector{UInt8})
    n > 0 || return false
    e > 0 || return false
    k = cld(ndigits(n; base=2), 8)
    length(signature) == k || return false
    s = parse(BigInt, "0x" * bytes2hex(signature))
    s < n || return false
    m = powermod(s, e, n)
    em_hex = string(m; base=16, pad=k * 2)
    em = hex2bytes(em_hex)
    length(em) == k || return false
    em[1] == 0x00 || return false
    em[2] == 0x01 || return false
    i = 3
    while i <= length(em) && em[i] == 0xFF
        i += 1
    end
    (i - 3) >= 8 || return false
    i <= length(em) && em[i] == 0x00 || return false
    i += 1
    expected = vcat(_SHA256_DIGEST_INFO_PREFIX, sha256(message))
    return em[i:end] == expected
end

function _cognito_jwks_url(user_pool_id::AbstractString, region::AbstractString)
    override = strip(get(ENV, "BIOCIRCUITS_EXPLORER_COGNITO_JWKS_URL_OVERRIDE", ""))
    isempty(override) || return override
    return "https://cognito-idp.$(region).amazonaws.com/$(user_pool_id)/.well-known/jwks.json"
end

function _read_jwks_body(url::AbstractString)
    if startswith(url, "file://")
        return read(url[8:end], String)
    elseif startswith(url, "http://") || startswith(url, "https://")
        response = HTTP.get(url; readtimeout=10, retry=false)
        Int(response.status) == 200 || error("Failed to fetch JWKs (HTTP $(response.status)): $(url)")
        return String(response.body)
    else
        error("Unsupported JWKs URL scheme: $(url)")
    end
end

function _fetch_jwks!(user_pool_id::AbstractString, region::AbstractString)
    url = _cognito_jwks_url(user_pool_id, region)
    body = _read_jwks_body(url)
    parsed = JSON3.read(body)
    keys_map = Dict{String, NamedTuple{(:n, :e), Tuple{BigInt, BigInt}}}()
    for jwk in parsed["keys"]
        String(get(jwk, "kty", "")) == "RSA" || continue
        kid = String(jwk["kid"])
        n = _b64url_decode_bigint(String(jwk["n"]))
        e = _b64url_decode_bigint(String(jwk["e"]))
        keys_map[kid] = (n=n, e=e)
    end
    _JWKS_CACHE[user_pool_id] = keys_map
    return keys_map
end

function _get_signing_key(user_pool_id::AbstractString, region::AbstractString, kid::AbstractString)
    lock(_JWKS_CACHE_LOCK) do
        keys_map = get(_JWKS_CACHE, user_pool_id, nothing)
        if keys_map === nothing
            keys_map = _fetch_jwks!(user_pool_id, region)
        end
        if !haskey(keys_map, kid)
            # Key rotation: refresh once on miss.
            keys_map = _fetch_jwks!(user_pool_id, region)
        end
        return get(keys_map, kid, nothing)
    end
end

# Test-only hook: pre-populate the JWKs cache so tests don't have to stand up
# a JWKs HTTP endpoint.
function _test_set_jwks!(user_pool_id::AbstractString, kid::AbstractString, n::BigInt, e::BigInt)
    lock(_JWKS_CACHE_LOCK) do
        keys_map = get!(_JWKS_CACHE, String(user_pool_id), Dict{String, NamedTuple{(:n, :e), Tuple{BigInt, BigInt}}}())
        keys_map[String(kid)] = (n=n, e=e)
    end
    return nothing
end

function _reset_jwks_cache!()
    lock(_JWKS_CACHE_LOCK) do
        empty!(_JWKS_CACHE)
    end
    return nothing
end

# Returns the verified claims dict, or throws ArgumentError on any failure.
function verify_cognito_jwt(token::AbstractString;
                            user_pool_id::AbstractString=String(strip(get(ENV, "BIOCIRCUITS_EXPLORER_COGNITO_USER_POOL_ID", ""))),
                            region::AbstractString=String(strip(get(ENV, "BIOCIRCUITS_EXPLORER_COGNITO_REGION", get(ENV, "AWS_REGION", "")))),
                            audience::AbstractString=String(strip(get(ENV, "BIOCIRCUITS_EXPLORER_COGNITO_APP_CLIENT_ID", ""))),
                            now_epoch::Real=time())
    isempty(user_pool_id) && throw(ArgumentError("Cognito not configured (BIOCIRCUITS_EXPLORER_COGNITO_USER_POOL_ID)"))
    isempty(region) && throw(ArgumentError("Cognito region not configured"))

    parts = split(strip(String(token)), '.')
    length(parts) == 3 || throw(ArgumentError("Malformed JWT (expected 3 dot-separated parts)"))
    header_b64, payload_b64, sig_b64 = parts

    header_bytes = _b64url_decode(header_b64)
    payload_bytes = _b64url_decode(payload_b64)
    sig_bytes = _b64url_decode(sig_b64)
    header = JSON3.read(String(header_bytes))
    payload = JSON3.read(String(payload_bytes))

    String(get(header, :alg, "")) == "RS256" || throw(ArgumentError("Unsupported JWT alg"))
    kid = String(get(header, :kid, ""))
    isempty(kid) && throw(ArgumentError("JWT missing kid"))

    key = _get_signing_key(user_pool_id, region, kid)
    key === nothing && throw(ArgumentError("Unknown JWT signing key id"))

    message = Vector{UInt8}(string(header_b64, ".", payload_b64))
    _rsa_pkcs1_v15_verify_sha256(key.n, key.e, message, sig_bytes) ||
        throw(ArgumentError("JWT signature invalid"))

    expected_iss = "https://cognito-idp.$(region).amazonaws.com/$(user_pool_id)"
    String(get(payload, :iss, "")) == expected_iss ||
        throw(ArgumentError("JWT iss mismatch"))

    token_use = String(get(payload, :token_use, ""))
    if token_use == "id"
        if !isempty(audience)
            String(get(payload, :aud, "")) == audience ||
                throw(ArgumentError("JWT aud mismatch"))
        end
    elseif token_use == "access"
        if !isempty(audience)
            String(get(payload, :client_id, "")) == audience ||
                throw(ArgumentError("JWT client_id mismatch"))
        end
    else
        throw(ArgumentError("Unsupported JWT token_use: $(token_use)"))
    end

    exp = Int(get(payload, :exp, 0))
    exp > Int(floor(Float64(now_epoch))) || throw(ArgumentError("JWT expired"))

    sub = String(get(payload, :sub, ""))
    isempty(sub) && throw(ArgumentError("JWT missing sub"))

    return Dict{String, Any}(
        "sub" => sub,
        "iss" => String(get(payload, :iss, "")),
        "exp" => exp,
        "token_use" => token_use,
        "email" => String(get(payload, :email, "")),
    )
end
