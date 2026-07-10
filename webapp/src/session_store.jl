module SessionStore

const DEFAULT_MAX_SESSIONS = 200
const DEFAULT_TTL_SECONDS  = 3600

const _SESSIONS = Dict{String, Any}()
const _SESSION_LAST_ACCESS = Dict{String, Float64}()
const _LOCK     = ReentrantLock()

function _evict_lru_if_full_unlocked!(key::String, max_sessions::Int)
    max_sessions > 0 || throw(ArgumentError("max_sessions must be positive"))
    haskey(_SESSIONS, key) && return nothing
    length(_SESSIONS) < max_sessions && return nothing

    # Sessions are convenience aliases for content-addressed NetworkIR
    # bundles, not durable state. Evict the least-recently-used alias rather
    # than turning a full cache into an unauthenticated 500/DoS.
    oldest_key = nothing
    oldest_access = Inf
    for existing_key in keys(_SESSIONS)
        accessed = get(_SESSION_LAST_ACCESS, existing_key, 0.0)
        if accessed < oldest_access
            oldest_key = existing_key
            oldest_access = accessed
        end
    end
    if oldest_key !== nothing
        delete!(_SESSIONS, oldest_key)
        delete!(_SESSION_LAST_ACCESS, oldest_key)
    end
    return nothing
end

function get_session(sid::AbstractString)
    lock(_LOCK) do
        key = String(sid)
        sess = get(_SESSIONS, key, nothing)
        sess === nothing && return nothing
        _SESSION_LAST_ACCESS[key] = time()
        return sess
    end
end

function set_session(sid::AbstractString, sess; max_sessions::Int = DEFAULT_MAX_SESSIONS)
    lock(_LOCK) do
        key = String(sid)
        _evict_lru_if_full_unlocked!(key, max_sessions)
        _SESSIONS[key] = sess
        _SESSION_LAST_ACCESS[key] = time()
        return sess
    end
end

function set_session_if_available(sid::AbstractString, sess;
                                  max_sessions::Int = DEFAULT_MAX_SESSIONS)
    lock(_LOCK) do
        key = String(sid)
        existing = get(_SESSIONS, key, nothing)
        if existing !== nothing
            _SESSION_LAST_ACCESS[key] = time()
            return existing === sess
        end
        _evict_lru_if_full_unlocked!(key, max_sessions)
        _SESSIONS[key] = sess
        _SESSION_LAST_ACCESS[key] = time()
        return true
    end
end

"""
    cleanup_expired_sessions!(; ttl, now_epoch, on_evict)

Drop sessions whose per-session access time is older than `ttl` seconds. `on_evict(sid)` is
called for each evicted session id after the internal lock is released.
Returns the list of evicted session ids.
"""
function cleanup_expired_sessions!(; ttl::Real = DEFAULT_TTL_SECONDS,
                                     now_epoch::Real = time(),
                                     on_evict = (sid) -> nothing)
    to_delete = lock(_LOCK) do
        to_delete = String[]
        for sid in keys(_SESSIONS)
            last_access = get(_SESSION_LAST_ACCESS, sid, 0.0)
            if now_epoch - last_access > ttl
                push!(to_delete, sid)
            end
        end
        for sid in to_delete
            delete!(_SESSIONS, sid)
            delete!(_SESSION_LAST_ACCESS, sid)
        end
        to_delete
    end
    foreach(on_evict, to_delete)
    return to_delete
end

session_count() = lock(_LOCK) do
    length(_SESSIONS)
end

# Test hook and controlled local-runtime reset.
_clear_all!() = lock(_LOCK) do
    empty!(_SESSIONS)
    empty!(_SESSION_LAST_ACCESS)
end

end # module
