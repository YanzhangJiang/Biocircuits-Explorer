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

# Loading a fitted network only constructs its equilibrium model. It does not
# enumerate regimes; those operations retain their own interactive budgets.
const MAX_DESIGN_MODEL_REACTIONS = 256
const MAX_DESIGN_MODEL_MONOMERS = 7
const MAX_DESIGN_MODEL_SPECIES = MAX_DESIGN_MODEL_REACTIONS + MAX_DESIGN_MODEL_MONOMERS
const MAX_DESIGN_MODEL_COMPLEX_SIZE = 64

function _request_model_build_mode(body)
    mode = _raw_get(body, :build_mode, "standard")
    mode isa AbstractString && mode in ("standard", "design_equilibrium") ||
        throw(ArgumentError("build_mode must be 'standard' or 'design_equilibrium'"))
    return Symbol(mode)
end

function _model_declared_free_species(network, species)
    referenced = Set(string.(species))
    extra = [sp for sp in network.species if !(sp.name in referenced)]
    all(sp -> sp.role in (:free, :auto), extra) || throw(ArgumentError(
        "A declared bound species must have a reaction that produces it"))
    return Symbol[Symbol(sp.name) for sp in extra]
end

function _design_equilibrium_structure(network, rules)
    1 <= length(rules) <= MAX_DESIGN_MODEL_REACTIONS || throw(ArgumentError(
        "design_equilibrium requires 1–$(MAX_DESIGN_MODEL_REACTIONS) reactions"))
    length(network.species) <= MAX_DESIGN_MODEL_SPECIES || throw(ArgumentError(
        "design_equilibrium supports at most $(MAX_DESIGN_MODEL_SPECIES) species"))
    all(rx -> rx.kind == :binding, network.reactions) || throw(ArgumentError(
        "design_equilibrium supports reversible binding reactions only"))
    _, referenced, free, products = parse_network_structure(rules)
    extra = _model_declared_free_species(network, referenced)
    free = sort!(vcat(free, extra))
    1 <= length(free) <= MAX_DESIGN_MODEL_MONOMERS || throw(ArgumentError(
        "design_equilibrium supports 1–$(MAX_DESIGN_MODEL_MONOMERS) free monomers"))
    species = vcat(free, products)
    length(species) <= MAX_DESIGN_MODEL_SPECIES || throw(ArgumentError(
        "design_equilibrium supports at most $(MAX_DESIGN_MODEL_SPECIES) species"))
    reactants, product_sides = parse_reactions(rules)
    producers = Set{Symbol}()
    for (rd, pd) in zip(reactants, product_sides)
        # One unique bimolecular formation step per complex makes precursor
        # closure and full reaction rank verifiable before compiling or hashing.
        (all(c -> 1 <= c <= 2, values(rd)) && sum(values(rd)) == 2 &&
         length(pd) == 1 && only(values(pd)) == 1) || throw(ArgumentError(
            "design_equilibrium requires two binding precursors and one product per reaction"))
        product = only(keys(pd))
        product in producers && throw(ArgumentError(
            "design_equilibrium requires one formation reaction per complex"))
        push!(producers, product)
    end
    supports = Dict(sym => [i == j ? 1 : 0 for i in eachindex(free)]
                    for (j, sym) in enumerate(free))
    pending = collect(eachindex(rules))
    while !isempty(pending)
        ready = [i for i in pending if all(sym -> haskey(supports, sym), keys(reactants[i]))]
        isempty(ready) && throw(ArgumentError(
            "design_equilibrium reactions must preserve an acyclic precursor closure"))
        for i in ready
            support = zeros(Int, length(free))
            for (sym, coefficient) in reactants[i]
                support .+= coefficient .* supports[sym]
            end
            sum(support) <= MAX_DESIGN_MODEL_COMPLEX_SIZE || throw(ArgumentError(
                "design_equilibrium complex size exceeds $(MAX_DESIGN_MODEL_COMPLEX_SIZE) monomers"))
            supports[only(keys(product_sides[i]))] = support
        end
        setdiff!(pending, ready)
    end
    # Include isolated declared free monomers. Pruning must not erase an input
    # or readout merely because its last binding reaction was removed.
    index = Dict(sym => i for (i, sym) in enumerate(species))
    full_N = zeros(Int, length(rules), length(species))
    for i in eachindex(rules)
        for (sym, coefficient) in reactants[i]
            full_N[i, index[sym]] += coefficient
        end
        full_N[i, index[only(keys(product_sides[i]))]] -= 1
    end
    L = hcat((supports[sym] for sym in species)...)
    return full_N, L, species, free, products
end

function _build_ir_model(network, rules, kd; design_structure=nothing)
    if design_structure !== nothing
        N, L, species, free, products = design_structure
        model = Bnc(N=N, L=L, x_sym=species,
                    q_sym=Symbol.("t" .* string.(free)),
                    K_sym=Symbol.("Kd" .* string.(eachindex(rules))))
        return model, species, free, products
    end
    _, species, _, _ = parse_network_structure(rules)
    extra = _model_declared_free_species(network, species)
    isempty(extra) && return build_model(rules, kd)
    # The same IR must compile identically regardless of which build mode first
    # populated its cache. Preserve declared isolated monomers for legacy loads
    # too, while retaining all ordinary request size and work guards.
    model, species, free, products = build_model(rules, kd)
    all_free = sort!(vcat(free, extra))
    all_species = vcat(all_free, products)
    index = Dict(sym => i for (i, sym) in enumerate(all_species))
    N = zeros(Int, model.r, length(all_species))
    L = zeros(Int, length(all_free), length(all_species))
    N[:, [index[sym] for sym in species]] .= Matrix(model.N)
    L[[findfirst(==(sym), all_free) for sym in free],
      [index[sym] for sym in species]] .= Matrix(model.L)
    for sym in extra
        L[findfirst(==(sym), all_free), index[sym]] = 1
    end
    result = Bnc(N=N, L=L, x_sym=all_species,
                 q_sym=Symbol.("t" .* string.(all_free)),
                 K_sym=Symbol.("Kd" .* string.(eachindex(rules))))
    return result, all_species, all_free, products
end

# A compiled Bnc model and its lazy SISO/geometry caches are mutable. Keep the
# lock outside the Dict it protects: acquiring a lock must not itself race with
# a concurrent insertion into that Dict.
mutable struct ModelBundle <: AbstractDict{String, Any}
    lock::ReentrantLock
    data::Dict{String, Any}
end

Base.getindex(bundle::ModelBundle, key::String) = bundle.data[key]
Base.setindex!(bundle::ModelBundle, value, key::String) = (bundle.data[key] = value)
Base.haskey(bundle::ModelBundle, key) = haskey(bundle.data, key)
Base.get(bundle::ModelBundle, key, default) = get(bundle.data, key, default)
Base.length(bundle::ModelBundle) = length(bundle.data)
Base.iterate(bundle::ModelBundle, state...) = iterate(bundle.data, state...)

# Legacy hand-built Dict bundles share one conservative fallback lock. All
# production bundles are `ModelBundle` values and therefore retain per-hash
# parallelism.
const _LEGACY_MODEL_BUNDLE_LOCK = ReentrantLock()

model_bundle_lock(bundle::ModelBundle) = bundle.lock
model_bundle_lock(bundle) = _LEGACY_MODEL_BUNDLE_LOCK

function with_model_bundle_lock(f::Function, bundle)
    lock(model_bundle_lock(bundle)) do
        f()
    end
end

# Cache misses for one content hash are single-flight. The small refcount keeps
# the lock entry alive while waiters exist (including when the builder throws),
# without retaining one lock forever for every hash ever submitted.
mutable struct _ModelBuildLockEntry
    lock::ReentrantLock
    users::Int
end

const _MODEL_BUILD_LOCKS = Dict{String, _ModelBuildLockEntry}()
const _MODEL_BUILD_LOCKS_GUARD = ReentrantLock()
const _REQUEST_MODEL_BUNDLE_TLS_KEY = Ref{Nothing}(nothing)

_current_request_model_bundle() =
    get(task_local_storage(), _REQUEST_MODEL_BUNDLE_TLS_KEY, nothing)

function _with_model_build_lock(f::Function, hash::AbstractString)
    key = String(hash)
    entry = lock(_MODEL_BUILD_LOCKS_GUARD) do
        current = get!(_MODEL_BUILD_LOCKS, key) do
            _ModelBuildLockEntry(ReentrantLock(), 0)
        end
        current.users += 1
        current
    end
    acquired = false
    try
        lock(entry.lock)
        acquired = true
        return f()
    finally
        acquired && unlock(entry.lock)
        lock(_MODEL_BUILD_LOCKS_GUARD) do
            entry.users -= 1
            entry.users == 0 && get(_MODEL_BUILD_LOCKS, key, nothing) === entry &&
                delete!(_MODEL_BUILD_LOCKS, key)
        end
    end
end

# Build (or fetch from cache) the bundle for a parsed NetworkIR, registering it
# in the content-addressed cache and the IR side-table. Throws ArgumentError on
# invalid kd. The returned bundle is the mutable Dict downstream handlers read
# from (and attach SISO path caches to).
function build_model_bundle(network::NetworkIR; synchronous::Bool=true,
                            build_mode::Symbol=:standard)
    # The router resolves and pins one bundle for the whole handler call. Reuse
    # it before any hashing; resolving the same build-model request twice would
    # otherwise repeat factorial exact-canonicalization work.
    request_bundle = _current_request_model_bundle()
    request_bundle !== nothing && return request_bundle

    # Do shape validation before content hashing. The canonical topology hash
    # explores base-species permutations, so hashing an adversarial high-d
    # network before the synchronous guard would itself be the expensive work
    # this boundary is meant to reject.
    bridge = network_ir_to_legacy_inputs(network)
    rules = collect(bridge.rules)
    kd = collect(bridge.kd)
    build_mode in (:standard, :design_equilibrium) || throw(ArgumentError("Unknown model build mode"))
    design_structure = build_mode === :design_equilibrium ?
        _design_equilibrium_structure(network, rules) : nothing
    if synchronous && build_mode === :standard
        enforce_sync_rule_budget(rules)
        length(network.species) <= MAX_SYNC_MODEL_N ||
            _sync_budget_exceeded("Declared model dimension exceeds the synchronous limit of $(MAX_SYNC_MODEL_N).")
    end
    all(x -> isfinite(x) && x > 0, kd) ||
        throw(ArgumentError("All Kd values must be finite and positive (> 0)"))

    h = network_ir_hash(network)
    check_budget(model) = build_mode === :design_equilibrium ? model :
        synchronous ? enforce_sync_model_budget(model) :
            model_candidate_bound(model; maximum=MAX_JOB_REGIME_CANDIDATES,
                                  label="Background model")
    cached = ModelCache.get_model(h)
    if cached !== nothing
        check_budget(cached["model"])
        return cached
    end

    return _with_model_build_lock(h) do
        # Another waiter may have completed the build while this task waited.
        cached = ModelCache.get_model(h)
        if cached !== nothing
            check_budget(cached["model"])
            return cached
        end

        model, species, free_syms, prod_syms = _build_ir_model(
            network, rules, kd; design_structure=design_structure)
        # Reaction/species counts can be rejected before construction; the
        # regime candidate product depends on the built helper. Check it before
        # publishing either the IR or compiled bundle so an over-budget model
        # cannot survive a 422 in the content-addressed cache.
        check_budget(model)
        network_dict = network_ir_to_dict(network)
        bundle = ModelBundle(ReentrantLock(), Dict{String, Any}(
            "model" => model,
            "species" => species,
            "free_syms" => free_syms,
            "prod_syms" => prod_syms,
            "kd" => kd,
            "rules" => rules,
            "network_ir" => network_dict,
            "network_ir_hash" => h,
        ))
        # Store the rebuild input before publishing the compiled object.
        ModelCache.put_ir(h, network_dict)
        ModelCache.put_model(h, bundle)
        return bundle
    end
end

# Resolve the compiled model bundle for a request body (resolution order above).
# Throws ModelResolutionError / IRValidationError / ArgumentError, which
# `_resolve_bundle_or_response` maps to HTTP responses.
function resolve_model_bundle(body)
    request_bundle = _current_request_model_bundle()
    request_bundle === nothing || return request_bundle

    if _raw_haskey(body, :network)
        return build_model_bundle(parse_network_ir(_raw_get(body, :network, nothing)))
    end

    requested_hash = nothing
    if _raw_haskey(body, :network_ir_hash)
        h = _request_network_ir_hash(_raw_get(body, :network_ir_hash, nothing))
        requested_hash = h
        cached = ModelCache.get_model(h)
        cached !== nothing && return cached
        ir = ModelCache.get_ir(h)
        ir !== nothing && return build_model_bundle(parse_network_ir(ir))
    end

    if _raw_haskey(body, :session_id)
        sid = _request_session_id(_raw_get(body, :session_id, nothing))
        sess = get_session(sid)
        if sess !== nothing && (requested_hash === nothing ||
                                get(sess, "network_ir_hash", nothing) == requested_hash)
            h = String(get(sess, "network_ir_hash", ""))
            if !isempty(h)
                cached = ModelCache.get_model(h)
                if cached === nothing
                    ModelCache.put_model(h, sess)
                elseif cached !== sess
                    # Repoint a stale session alias at the current canonical
                    # cache object after TTL/LRU rebuild.
                    set_session(sid, cached)
                    sess = cached
                end
            end
            return sess
        end
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

# Ordinary HTTP handlers are synchronized here rather than each handler growing
# its own subtly different lock boundary. Handlers that merely copy rules still
# participate because a concurrent SISO handler may resize the shared Dict.
const MODEL_BUNDLE_HANDLER_NAMES = Set{Symbol}((
    :handle_build_model,
    :handle_find_vertices,
    :handle_build_graph,
    :handle_siso_paths,
    :handle_siso_polyhedra,
    :handle_siso_path_condition,
    :handle_siso_trajectory,
    :handle_behavior_families,
    :handle_phenotype_classify,
    :handle_rop_cloud,
    :handle_vertex_detail,
    :handle_fret_heatmap,
    :handle_parameter_scan_1d,
    :handle_parameter_scan_2d,
    :handle_atlas_landscape_2d,
    :handle_rop_polyhedron,
    :handle_place_parameters,
    :handle_placer_menu,
    :handle_placer_curve,
    :handle_placer_threshold,
    :handle_placer_realize_program,
    :handle_placer_level,
    :handle_ro_field,
))

function _request_model_bundle(handler_name::Symbol, body)
    if handler_name === :handle_build_model
        _raw_haskey(body, :session_id) &&
            _request_session_id(_raw_get(body, :session_id, nothing))
        payload = _raw_haskey(body, :network) ? _raw_get(body, :network, nothing) : body
        return build_model_bundle(parse_network_ir(payload);
                                  build_mode=_request_model_build_mode(body))
    end
    if _raw_haskey(body, :network) || _raw_haskey(body, :network_ir_hash) ||
       _raw_haskey(body, :session_id)
        return resolve_model_bundle(body)
    end
    return nothing
end

function with_request_model_bundle_lock(f::Function, handler_name::Symbol, req)
    handler_name in MODEL_BUNDLE_HANDLER_NAMES || return f()
    body = try
        read_json(req)
    catch err
        err isa RequestBodyTooLarge && rethrow(err)
        # Let the handler/error mapper produce the canonical validation response.
        return f()
    end
    bundle = try
        _request_model_bundle(handler_name, body)
    catch err
        if err isa ModelResolutionError || err isa IRValidationError || err isa ArgumentError
            return f()
        end
        rethrow(err)
    end
    bundle === nothing && return f()
    return task_local_storage(_REQUEST_MODEL_BUNDLE_TLS_KEY, bundle) do
        with_model_bundle_lock(bundle) do
            design_load = handler_name === :handle_build_model &&
                _request_model_build_mode(body) === :design_equilibrium
            haskey(bundle, "model") && !design_load && enforce_sync_model_budget(bundle["model"])
            f()
        end
    end
end
