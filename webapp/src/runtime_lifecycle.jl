# ─── Global state ───
const SESSION_TTL = SessionStore.DEFAULT_TTL_SECONDS
const SESSION_CLEANUP_INTERVAL = 300  # 5 minutes

# Process startup timestamp, captured monotonically so /health uptime values
# stay sane across NTP adjustments. Initialized in `__init__`; zero before
# that, which `handle_health` interprets as "module not yet initialized" and
# `handle_ready` uses as a readiness gate.
const _STARTUP_TIME_NS = Ref{UInt64}(0)

function __init__()
    _STARTUP_TIME_NS[] = time_ns()
end

function parse_optional_int(raw::AbstractString)
    text = strip(String(raw))
    isempty(text) && return nothing
    try
        return parse(Int, text)
    catch
        return nothing
    end
end

function configured_parent_pid()
    parsed = parse_optional_int(Config.parent_pid_raw())
    return parsed !== nothing && parsed > 0 ? parsed : nothing
end

current_parent_pid() = Int(ccall(:getppid, Cint, ()))

function parent_watchdog_should_exit(expected_parent_pid::Union{Nothing, Int}, actual_parent_pid::Integer)
    expected_parent_pid === nothing && return false
    expected_parent_pid <= 0 && return false
    return actual_parent_pid <= 1 || actual_parent_pid != expected_parent_pid
end

function parent_watchdog_loop(expected_parent_pid::Int; interval_seconds::Real=2.0)
    @info "Parent watchdog enabled" expected_parent_pid interval_seconds

    while true
        sleep(interval_seconds)
        actual_parent_pid = current_parent_pid()
        if parent_watchdog_should_exit(expected_parent_pid, actual_parent_pid)
            append_debug_log(
                "INFO",
                "Parent watchdog exiting orphaned backend";
                module_name=:BiocircuitsExplorerBackend,
                details="expected_parent_pid=$(expected_parent_pid), actual_parent_pid=$(actual_parent_pid)",
            )
            @info "Parent watchdog exiting orphaned backend" expected_parent_pid actual_parent_pid
            flush(stdout)
            flush(stderr)
            Base.exit(0)
        end
    end
end

resolve_port() = Config.port()

function _validate_bind_host(raw::AbstractString)
    host = strip(String(raw))
    isempty(host) && throw(ArgumentError("Bind host must not be empty."))
    ncodeunits(host) <= 253 || throw(ArgumentError("Bind host is too long."))
    all(c -> isletter(c) || isdigit(c) || c in ('.', '-', '_', ':', '%'), host) ||
        throw(ArgumentError("Invalid bind host: $(repr(host)). Pass a hostname or IP address without a scheme, path, or port."))
    if count(==(':'), host) == 1
        throw(ArgumentError("Invalid bind host: $(repr(host)). Pass the port separately."))
    end
    return host
end

function resolve_host(expected_parent_pid::Union{Nothing, Int}=configured_parent_pid())
    configured = Config.host_override()
    if !isempty(configured)
        return _validate_bind_host(configured)
    end
    return expected_parent_pid === nothing ? "0.0.0.0" : "127.0.0.1"
end

# Session cleanup task — delegates expiry to SessionStore, then prunes the
# debug-log buffers attached to long-idle clients.
function cleanup_old_sessions()
    while true
        sleep(SESSION_CLEANUP_INTERVAL)
        current_time = time()

        SessionStore.cleanup_expired_sessions!(
            ttl = SESSION_TTL,
            now_epoch = current_time,
            on_evict = sid -> (@info "Cleaned up expired session: $sid"),
        )

        # Compiled model bundles are a pure derived cache of the NetworkIR; expire
        # them on the same TTL. The IR side-table is left intact so an expired
        # model can be rebuilt on demand from its hash.
        ModelCache.cleanup_expired_models!(
            ttl = SESSION_TTL,
            now_epoch = current_time,
        )

        DebugLog.cleanup_expired_clients!(ttl = SESSION_TTL, now_epoch = current_time)
    end
end
