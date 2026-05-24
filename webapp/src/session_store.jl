module SessionStore

const DEFAULT_MAX_SESSIONS = 200
const DEFAULT_TTL_SECONDS  = 3600

const _SESSIONS = Dict{String, Any}()
const _LOCK     = ReentrantLock()

function get_session(sid::AbstractString)
    lock(_LOCK) do
        key = String(sid)
        sess = get(_SESSIONS, key, nothing)
        sess === nothing && return nothing
        sess["last_access"] = time()
        return sess
    end
end

function set_session(sid::AbstractString, sess; max_sessions::Int = DEFAULT_MAX_SESSIONS)
    lock(_LOCK) do
        key = String(sid)
        if !haskey(_SESSIONS, key) && length(_SESSIONS) >= max_sessions
            error("Too many active sessions (limit: $(max_sessions)). Please try again later.")
        end
        sess["last_access"] = time()
        _SESSIONS[key] = sess
        return sess
    end
end

"""
    cleanup_expired_sessions!(; ttl, now_epoch, on_evict)

Drop sessions whose `last_access` is older than `ttl` seconds. `on_evict(sid)` is
called for each evicted session id, while the internal lock is still held.
Returns the list of evicted session ids.
"""
function cleanup_expired_sessions!(; ttl::Real = DEFAULT_TTL_SECONDS,
                                     now_epoch::Real = time(),
                                     on_evict = (sid) -> nothing)
    lock(_LOCK) do
        to_delete = String[]
        for (sid, sess) in _SESSIONS
            last_access = get(sess, "last_access", 0.0)
            if now_epoch - last_access > ttl
                push!(to_delete, sid)
            end
        end
        for sid in to_delete
            delete!(_SESSIONS, sid)
            on_evict(sid)
        end
        return to_delete
    end
end

session_count() = lock(_LOCK) do
    length(_SESSIONS)
end

end # module
