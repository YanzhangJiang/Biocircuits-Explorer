module DebugLog

using Logging
using Dates

const DEBUG_LOG_LIMIT = 1500

const DEBUG_LOGS                = Vector{Dict{String, Any}}()
const DEBUG_LOG_LOCK            = ReentrantLock()
const DEBUG_LOG_SEQ             = Ref(0)
const DEBUG_LOGGER_INSTALLED    = Ref(false)
const DEBUG_LOGS_BY_CLIENT      = Dict{String, Vector{Dict{String, Any}}}()
const DEBUG_CLIENT_LAST_ACCESS  = Dict{String, Float64}()
const DEBUG_TASK_CLIENTS        = IdDict{Task, String}()

function _stringify_log_value(value)
    if value isa AbstractString
        return value
    elseif value isa Symbol
        return String(value)
    else
        return sprint(show, value)
    end
end

function _request_header(req, name::AbstractString)
    target = lowercase(name)
    for (key, value) in req.headers
        lowercase(String(key)) == target && return String(value)
    end
    return nothing
end

function normalize_debug_client_id(value)
    value === nothing && return nothing
    text = strip(String(value))
    isempty(text) && return nothing
    return text
end

function debug_client_id_from_request(req)
    primary = _request_header(req, "X-Biocircuits-Explorer-Debug-Client")
    fallback = primary === nothing ? _request_header(req, "X-ROP-Debug-Client") : primary
    return normalize_debug_client_id(fallback)
end

function with_debug_client_scope(f::Function, client_id)
    task = current_task()
    previous = nothing
    had_previous = false

    lock(DEBUG_LOG_LOCK) do
        had_previous = haskey(DEBUG_TASK_CLIENTS, task)
        if had_previous
            previous = DEBUG_TASK_CLIENTS[task]
        end
        if client_id === nothing
            delete!(DEBUG_TASK_CLIENTS, task)
        else
            DEBUG_TASK_CLIENTS[task] = client_id
            DEBUG_CLIENT_LAST_ACCESS[client_id] = time()
        end
    end

    try
        return f()
    finally
        lock(DEBUG_LOG_LOCK) do
            if had_previous
                DEBUG_TASK_CLIENTS[task] = previous
            else
                delete!(DEBUG_TASK_CLIENTS, task)
            end
        end
    end
end

with_debug_client_scope(client_id, f::Function) = with_debug_client_scope(f, client_id)

function _select_buffer(client_id)
    client_id === nothing && return DEBUG_LOGS
    return get(DEBUG_LOGS_BY_CLIENT, client_id, Dict{String, Any}[])
end

function append_debug_log(level, message;
                          module_name=nothing, group=nothing,
                          file=nothing, line=nothing, details=nothing)
    record = Dict{String, Any}(
        "timestamp" => Dates.format(now(), dateformat"yyyy-mm-dd HH:MM:SS.s"),
        "level" => uppercase(string(level)),
        "message" => _stringify_log_value(message),
    )
    module_name === nothing || (record["module"] = _stringify_log_value(module_name))
    group === nothing || (record["group"] = _stringify_log_value(group))
    file === nothing || (record["file"] = _stringify_log_value(file))
    line === nothing || (record["line"] = line)
    details === nothing || isempty(String(details)) || (record["details"] = String(details))

    lock(DEBUG_LOG_LOCK) do
        DEBUG_LOG_SEQ[] += 1
        record["seq"] = DEBUG_LOG_SEQ[]
        push!(DEBUG_LOGS, record)
        if length(DEBUG_LOGS) > DEBUG_LOG_LIMIT
            deleteat!(DEBUG_LOGS, 1:(length(DEBUG_LOGS) - DEBUG_LOG_LIMIT))
        end

        client_id = get(DEBUG_TASK_CLIENTS, current_task(), nothing)
        if client_id !== nothing
            entries = get!(DEBUG_LOGS_BY_CLIENT, client_id) do
                Dict{String, Any}[]
            end
            push!(entries, copy(record))
            if length(entries) > DEBUG_LOG_LIMIT
                deleteat!(entries, 1:(length(entries) - DEBUG_LOG_LIMIT))
            end
            DEBUG_CLIENT_LAST_ACCESS[client_id] = time()
        end
    end
    return nothing
end

struct BufferingConsoleLogger <: AbstractLogger
    console::AbstractLogger
    min_level::LogLevel
    console_forwarding_enabled::Base.RefValue{Bool}
end

BufferingConsoleLogger(console::AbstractLogger, min_level::LogLevel) =
    BufferingConsoleLogger(console, min_level, Ref(true))

Logging.min_enabled_level(logger::BufferingConsoleLogger) = logger.min_level
Logging.shouldlog(logger::BufferingConsoleLogger, level, _module, group, id) = level >= logger.min_level
Logging.catch_exceptions(::BufferingConsoleLogger) = true

function Logging.handle_message(logger::BufferingConsoleLogger, level, message, _module, group, id, file, line; kwargs...)
    details = String[]
    for (key, value) in kwargs
        if key === :exception
            if value isa Tuple && length(value) >= 2
                push!(details, sprint(showerror, value[1], value[2]))
            else
                push!(details, _stringify_log_value(value))
            end
        else
            push!(details, string(key) * " = " * _stringify_log_value(value))
        end
    end
    append_debug_log(level, message;
        module_name=_module,
        group=group,
        file=file,
        line=line,
        details=isempty(details) ? nothing : join(details, "\n"),
    )

    if logger.console_forwarding_enabled[]
        try
            Logging.handle_message(logger.console, level, message, _module, group, id, file, line; kwargs...)
        catch err
            logger.console_forwarding_enabled[] = false
            append_debug_log("WARN", "Console log forwarding disabled after logger write failure";
                module_name=:DebugLog,
                details=sprint(showerror, err, catch_backtrace()),
            )
        end
    end
end

function install_debug_logger!()
    DEBUG_LOGGER_INSTALLED[] && return
    DEBUG_LOGGER_INSTALLED[] = true
    global_logger(BufferingConsoleLogger(current_logger(), Logging.Info))
    append_debug_log("INFO", "Debug log capture initialized"; module_name=:DebugLog)
end

"""
    read_logs(; after_seq, limit, client_id)

Read up to `limit` debug-log entries. If `client_id` is supplied, reads from
that client's per-task buffer (and refreshes its last-access timestamp);
otherwise reads the global buffer. Returns a NamedTuple
`(entries, next_seq, total, limit)`.
"""
function read_logs(; after_seq::Integer = 0, limit::Integer = 300, client_id = nothing)
    after_seq = max(0, Int(after_seq))
    limit = clamp(Int(limit), 1, DEBUG_LOG_LIMIT)

    entries = Dict{String, Any}[]
    next_seq = 0
    total = 0
    lock(DEBUG_LOG_LOCK) do
        logs = _select_buffer(client_id)
        client_id === nothing || (DEBUG_CLIENT_LAST_ACCESS[client_id] = time())
        total = length(logs)
        if after_seq > 0
            entries = [copy(entry) for entry in logs if Int(get(entry, "seq", 0)) > after_seq]
        else
            start_idx = max(1, total - limit + 1)
            entries = [copy(entry) for entry in @view logs[start_idx:end]]
        end
        if !isempty(logs)
            next_seq = Int(get(logs[end], "seq", 0))
        end
    end

    if after_seq > 0 && length(entries) > limit
        entries = entries[end-limit+1:end]
    end

    return (entries = entries, next_seq = next_seq, total = total, limit = limit)
end

"""
    cleanup_expired_clients!(; ttl, now_epoch)

Drop per-client log buffers whose client hasn't queried in `ttl` seconds.
Returns the list of evicted client ids.
"""
function cleanup_expired_clients!(; ttl::Real = 3600, now_epoch::Real = time())
    lock(DEBUG_LOG_LOCK) do
        expired_clients = String[]
        for (client_id, last_access) in DEBUG_CLIENT_LAST_ACCESS
            if now_epoch - last_access > ttl
                push!(expired_clients, client_id)
            end
        end
        for client_id in expired_clients
            delete!(DEBUG_CLIENT_LAST_ACCESS, client_id)
            delete!(DEBUG_LOGS_BY_CLIENT, client_id)
        end
        return expired_clients
    end
end

end # module
