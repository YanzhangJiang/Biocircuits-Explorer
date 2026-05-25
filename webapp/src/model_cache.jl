module ModelCache

# Content-addressed cache of compiled model bundles, keyed by `network_ir_hash`.
#
# A "bundle" is the same mutable Dict the session used to hold — `model`,
# `species`, `free_syms`, `prod_syms`, `kd`, `rules`, `network_ir`
# (+ lazily-attached SISO path caches like `siso_<qK>`). Keying it by the IR
# hash makes the compiled model a *pure derived cache* of the NetworkIR:
# identical IRs share one bundle, and a bundle can be rebuilt from the IR on a
# miss (server restart, LRU eviction). The `session_id` becomes a convenience
# handle pointing at the same object (see SessionStore).
#
# A lighter `_IRS` side-table keeps the NetworkIR dict by hash so a bundle can
# be rebuilt even after the (heavier) model bundle is evicted. IRs are small, so
# they get a much larger cap. This is the in-memory precursor to a durable
# IR-by-hash store (e.g. SQLite) should multi-replica / cross-restart
# persistence ever be needed.

const DEFAULT_MAX_MODELS  = 200
const DEFAULT_MAX_IRS     = 2000
const DEFAULT_TTL_SECONDS = 3600

const _MODELS = Dict{String, Any}()   # network_ir_hash => bundle (mutable Dict)
const _IRS    = Dict{String, Any}()   # network_ir_hash => network_ir dict
const _LOCK   = ReentrantLock()

function get_model(hash::AbstractString)
    lock(_LOCK) do
        bundle = get(_MODELS, String(hash), nothing)
        bundle === nothing && return nothing
        bundle isa AbstractDict && (bundle["last_access"] = time())
        return bundle
    end
end

function put_model(hash::AbstractString, bundle; max_models::Int = DEFAULT_MAX_MODELS)
    lock(_LOCK) do
        key = String(hash)
        if !haskey(_MODELS, key) && length(_MODELS) >= max_models
            _evict_lru_model!()
        end
        bundle isa AbstractDict && (bundle["last_access"] = time())
        _MODELS[key] = bundle
        return bundle
    end
end

get_ir(hash::AbstractString) = lock(_LOCK) do
    get(_IRS, String(hash), nothing)
end

function put_ir(hash::AbstractString, ir; max_irs::Int = DEFAULT_MAX_IRS)
    lock(_LOCK) do
        key = String(hash)
        if !haskey(_IRS, key) && length(_IRS) >= max_irs
            delete!(_IRS, first(keys(_IRS)))
        end
        _IRS[key] = ir
        return ir
    end
end

# Evict the least-recently-accessed model entry. Caller holds `_LOCK`.
function _evict_lru_model!()
    isempty(_MODELS) && return
    oldest_key = nothing
    oldest_t = Inf
    for (k, v) in _MODELS
        t = (v isa AbstractDict) ? get(v, "last_access", 0.0) : 0.0
        if t < oldest_t
            oldest_t = t
            oldest_key = k
        end
    end
    oldest_key !== nothing && delete!(_MODELS, oldest_key)
end

"""
    cleanup_expired_models!(; ttl, now_epoch, on_evict)

Drop model bundles whose `last_access` is older than `ttl` seconds. The IR
side-table is intentionally *not* expired here (IRs are cheap and let evicted
models be rebuilt on demand); it is bounded only by its cap. Returns the list of
evicted hashes.
"""
function cleanup_expired_models!(; ttl::Real = DEFAULT_TTL_SECONDS,
                                   now_epoch::Real = time(),
                                   on_evict = (h) -> nothing)
    lock(_LOCK) do
        to_delete = String[]
        for (h, bundle) in _MODELS
            last_access = (bundle isa AbstractDict) ? get(bundle, "last_access", 0.0) : 0.0
            now_epoch - last_access > ttl && push!(to_delete, h)
        end
        for h in to_delete
            delete!(_MODELS, h)
            on_evict(h)
        end
        return to_delete
    end
end

model_count() = lock(_LOCK) do; length(_MODELS); end
ir_count()    = lock(_LOCK) do; length(_IRS); end

# Test hooks. `_clear_models!` simulates LRU/restart eviction of compiled models
# while keeping IRs, so the rebuild-from-IR path can be exercised; `_clear_all!`
# simulates a cold start.
_clear_models!() = lock(_LOCK) do; empty!(_MODELS); end
_clear_all!()    = lock(_LOCK) do; empty!(_MODELS); empty!(_IRS); end

end # module
