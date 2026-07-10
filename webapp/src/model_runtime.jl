# ─── Model bundle resolution (content-addressed; session as a legacy alias) ────
#
# Downstream endpoints used to hard-require a live `session_id` and 404 on miss.
# They now resolve a compiled model *bundle* from any of three inputs, in
# priority order, rebuilding transparently when only the IR/hash is known:
#   1. `network`         — a NetworkIR (or legacy {reactions,kd}); fully
#                          stateless. Served from the content-addressed cache,
#                          or built and cached.
#   2. `network_ir_hash` — cache key. Served from cache, or rebuilt from the IR
#                          side-table when the compiled model was evicted.
#   3. `session_id`      — legacy handle; served while the session is alive.
# On an unrecoverable miss, `ModelResolutionError(need_network=true)` tells the
# client to resend `network` and retry.
struct ModelResolutionError <: Exception
    msg::String
    status::Int
    need_network::Bool
end
ModelResolutionError(msg::AbstractString; status::Integer = 409, need_network::Bool = true) =
    ModelResolutionError(String(msg), Int(status), need_network)
Base.showerror(io::IO, err::ModelResolutionError) = print(io, err.msg)

# Build (or fetch from cache) the bundle for a parsed NetworkIR, registering it
# in the content-addressed cache and the IR side-table. Throws ArgumentError on
# invalid kd. The returned bundle is the mutable Dict downstream handlers read
# from (and attach SISO path caches to).
function build_model_bundle(network::NetworkIR)
    h = network_ir_hash(network)
    cached = ModelCache.get_model(h)
    cached !== nothing && return cached

    bridge = network_ir_to_legacy_inputs(network)
    rules = collect(bridge.rules)
    kd = collect(bridge.kd)
    any(x -> x <= 0, kd) && throw(ArgumentError("All Kd values must be positive (> 0)"))

    model, species, free_syms, prod_syms = build_model(rules, kd)
    network_dict = network_ir_to_dict(network)
    bundle = Dict{String, Any}(
        "model" => model,
        "species" => species,
        "free_syms" => free_syms,
        "prod_syms" => prod_syms,
        "kd" => kd,
        "rules" => rules,
        "network_ir" => network_dict,
        "network_ir_hash" => h,
    )
    ModelCache.put_model(h, bundle)
    ModelCache.put_ir(h, network_dict)
    return bundle
end

# Resolve the compiled model bundle for a request body (resolution order above).
# Throws ModelResolutionError / IRValidationError / ArgumentError, which
# `_resolve_bundle_or_response` maps to HTTP responses.
function resolve_model_bundle(body)
    if _raw_haskey(body, :network)
        return build_model_bundle(parse_network_ir(_raw_get(body, :network, nothing)))
    end

    if _raw_haskey(body, :network_ir_hash)
        h = String(_raw_get(body, :network_ir_hash, ""))
        if !isempty(h)
            cached = ModelCache.get_model(h)
            cached !== nothing && return cached
            ir = ModelCache.get_ir(h)
            ir !== nothing && return build_model_bundle(parse_network_ir(ir))
        end
    end

    if _raw_haskey(body, :session_id)
        sess = get_session(String(_raw_get(body, :session_id, "")))
        sess !== nothing && return sess
    end

    throw(ModelResolutionError(
        "No live model for this request. Resend `network` (the NetworkIR) to rebuild."))
end

# Resolve a bundle, returning `(bundle, nothing)` or `(nothing, http_response)`
# so handlers can early-return the error: `bundle, err = ...; err === nothing || return err`.
function _resolve_bundle_or_response(body)
    try
        return (resolve_model_bundle(body), nothing)
    catch err
        if err isa ModelResolutionError
            payload = Dict{String, Any}("error" => err.msg)
            err.need_network && (payload["need_network"] = true)
            return (nothing, json_response(payload; status = err.status))
        elseif err isa IRValidationError
            return (nothing, error_response(sprint(showerror, err); status = 400))
        elseif err isa ArgumentError
            return (nothing, error_response(sprint(showerror, err); status = 400))
        end
        rethrow(err)
    end
end
