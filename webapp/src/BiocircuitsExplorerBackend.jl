module BiocircuitsExplorerBackend

export main, julia_main, router
export AtlasSearchProfile, AtlasBehaviorConfig, AtlasEnumerationSpec, AtlasChangeExpansionSpec, AtlasQuerySpec, InverseDesignSpec, InverseRefinementSpec
export atlas_search_profile_binding_small_v0, atlas_behavior_config_default
export atlas_enumeration_spec_default, atlas_change_expansion_spec_default, atlas_query_spec_default, inverse_design_spec_default, inverse_refinement_spec_default
export atlas_library_default, is_atlas_library
export atlas_sqlite_default_path, atlas_sqlite_connect, atlas_sqlite_init!, atlas_sqlite_has_library
export atlas_sqlite_load_library, atlas_sqlite_save_library!, atlas_sqlite_summary
export atlas_sqlite_existing_ok_slice_ids, atlas_sqlite_merge_atlas!, atlas_sqlite_record_skip_only_event!, atlas_sqlite_append_atlas!
export canonical_program_profile, encode_program_blob, decode_program_blob, behavior_program_hash, program_exact_label, program_motif_label, program_features
export enumerate_network_specs
export build_behavior_atlas, build_behavior_atlas_from_spec
export build_atlas_library, build_atlas_library_from_spec
export merge_atlas_library, merge_atlas_library_from_spec
export query_behavior_atlas, query_behavior_atlas_from_spec
export run_inverse_design, run_inverse_design_from_spec
export compile_query, stable_hash
export canonicalize_network, emit_support_signature
export run_bounding_screen, run_exact_support_screen
export plan_delta_build, build_summary_delta, merge_atlas_delta
export retrieve_candidates, materialize_witnesses, refine_top_k
export record_negative, check_negative

using HTTP
using JSON3
using LinearAlgebra
using BindingAndCatalysis
using Polyhedra
using CDDLib
using Graphs
using SparseArrays
using Random
using Logging
using Dates
using Base64
using SHA
using DBInterface
using SQLite

# ─── Global state ───
const MODELS = Dict{String, Any}()  # session_id => Dict with model + computed data
const MODELS_LOCK = ReentrantLock()  # protects concurrent access to MODELS
const MAX_SESSIONS = 200  # prevent unbounded memory growth
const STATIC_DIR = Ref{Union{Nothing, String}}(nothing)
const SESSION_TTL = 3600  # 1 hour in seconds
const SESSION_CLEANUP_INTERVAL = 300  # 5 minutes
const DEBUG_LOGS = Vector{Dict{String, Any}}()
const DEBUG_LOG_LOCK = ReentrantLock()
const DEBUG_LOG_SEQ = Ref(0)
const DEBUG_LOG_LIMIT = 1500
const DEBUG_LOGGER_INSTALLED = Ref(false)
const DEBUG_LOGS_BY_CLIENT = Dict{String, Vector{Dict{String, Any}}}()
const DEBUG_CLIENT_LAST_ACCESS = Dict{String, Float64}()
const DEBUG_TASK_CLIENTS = IdDict{Task, String}()

function _stringify_log_value(value)
    if value isa AbstractString
        return value
    elseif value isa Symbol
        return String(value)
    else
        return sprint(show, value)
    end
end

function request_header(req, name::AbstractString)
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
    return normalize_debug_client_id(
        request_header(req, "X-Biocircuits-Explorer-Debug-Client")
        |> value -> value === nothing ? request_header(req, "X-ROP-Debug-Client") : value
    )
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

function first_nonempty_env(keys::Vector{String})
    for key in keys
        value = strip(get(ENV, key, ""))
        !isempty(value) && return value
    end
    return ""
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

configured_parent_pid() = parse_optional_int(first_nonempty_env(["BIOCIRCUITS_EXPLORER_PARENT_PID", "ROP_PARENT_PID"]))

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

function select_debug_log_buffer(client_id)
    if client_id === nothing
        return DEBUG_LOGS
    end
    return get(DEBUG_LOGS_BY_CLIENT, client_id, Dict{String, Any}[])
end

function append_debug_log(level, message; module_name=nothing, group=nothing, file=nothing, line=nothing, details=nothing)
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
                module_name=:BiocircuitsExplorerBackend,
                details=sprint(showerror, err, catch_backtrace()),
            )
        end
    end
end

function install_debug_logger!()
    DEBUG_LOGGER_INSTALLED[] && return
    DEBUG_LOGGER_INSTALLED[] = true
    global_logger(BufferingConsoleLogger(current_logger(), Logging.Info))
    append_debug_log("INFO", "Debug log capture initialized"; module_name=:BiocircuitsExplorerBackend)
end

function resolve_static_dir()
    candidates = String[]

    env_static_dir = first_nonempty_env(["BIOCIRCUITS_EXPLORER_PUBLIC_DIR", "ROP_PUBLIC_DIR"])
    if !isempty(env_static_dir)
        push!(candidates, abspath(expanduser(env_static_dir)))
    end

    program_file = try
        Base.PROGRAM_FILE
    catch
        ""
    end

    if !isempty(program_file)
        exe_dir = dirname(abspath(program_file))
        append!(candidates, [
            normpath(joinpath(exe_dir, "..", "share", "biocircuits-explorer", "public")),
            normpath(joinpath(exe_dir, "..", "Resources", "public")),
            normpath(joinpath(exe_dir, "..", "resources", "public")),
        ])
    end

    push!(candidates, normpath(joinpath(@__DIR__, "..", "public")))

    for candidate in unique(candidates)
        isdir(candidate) && return candidate
    end

    error("Could not locate web assets. Checked: $(join(unique(candidates), ", "))")
end

function static_dir()
    if STATIC_DIR[] === nothing
        STATIC_DIR[] = resolve_static_dir()
    end
    return STATIC_DIR[]::String
end

resolve_port() = parse(Int, isempty(first_nonempty_env(["BIOCIRCUITS_EXPLORER_PORT", "ROP_PORT"])) ? "8088" : first_nonempty_env(["BIOCIRCUITS_EXPLORER_PORT", "ROP_PORT"]))

# Thread-safe session helpers
function get_session(sid::AbstractString)
    lock(MODELS_LOCK) do
        sess = get(MODELS, sid, nothing)
        if sess !== nothing
            sess["last_access"] = time()
        end
        return sess
    end
end

function set_session(sid::AbstractString, sess)
    lock(MODELS_LOCK) do
        if !haskey(MODELS, sid) && length(MODELS) >= MAX_SESSIONS
            error("Too many active sessions (limit: $MAX_SESSIONS). Please try again later.")
        end
        sess["last_access"] = time()
        MODELS[sid] = sess
    end
end

# Session cleanup task
function cleanup_old_sessions()
    while true
        sleep(SESSION_CLEANUP_INTERVAL)
        current_time = time()
        expired_clients = String[]

        lock(MODELS_LOCK) do
            to_delete = String[]
            for (sid, sess) in MODELS
                last_access = get(sess, "last_access", 0.0)
                if current_time - last_access > SESSION_TTL
                    push!(to_delete, sid)
                end
            end
            for sid in to_delete
                delete!(MODELS, sid)
                @info "Cleaned up expired session: $sid"
            end
        end

        lock(DEBUG_LOG_LOCK) do
            for (client_id, last_access) in DEBUG_CLIENT_LAST_ACCESS
                if current_time - last_access > SESSION_TTL
                    push!(expired_clients, client_id)
                end
            end
            for client_id in expired_clients
                delete!(DEBUG_CLIENT_LAST_ACCESS, client_id)
                delete!(DEBUG_LOGS_BY_CLIENT, client_id)
            end
        end
    end
end

# ─── Reaction parser (same logic as notebooks) ───
const ARROW_RE = r"<->|<=>|↔"

function parse_term(term::AbstractString)
    t = strip(term)
    isempty(t) && error("Empty term")
    m = match(r"^([0-9]+)?\s*([A-Za-z][A-Za-z0-9_]*)$", t)
    m === nothing && error("Bad term: $term")
    coeff = m.captures[1] === nothing ? 1 : parse(Int, m.captures[1])
    sym = Symbol(m.captures[2])
    return sym, coeff
end

function parse_side(side::AbstractString)
    parts = split(side, "+")
    dict = Dict{Symbol,Int}()
    for p in parts
        sym, coeff = parse_term(p)
        dict[sym] = get(dict, sym, 0) + coeff
    end
    return dict
end

function parse_reactions(rules::Vector{String})
    reactants = Vector{Dict{Symbol,Int}}()
    products  = Vector{Dict{Symbol,Int}}()
    for rule in rules
        m = match(ARROW_RE, rule)
        m === nothing && error("Reaction must contain '<->' or '<=>' or '↔': $rule")
        left, right = split(rule, m.match)
        push!(reactants, parse_side(left))
        push!(products, parse_side(right))
    end
    return reactants, products
end

function parse_network_structure(rules::Vector{String})
    reactants, products = parse_reactions(rules)
    r = length(rules)

    all_species = Set{Symbol}()
    for rd in reactants
        union!(all_species, keys(rd))
    end
    for pd in products
        union!(all_species, keys(pd))
    end

    # Species that appear on product side are treated as bound species.
    prod_species_set = Set{Symbol}()
    for pd in products
        union!(prod_species_set, keys(pd))
    end
    free_syms = sort([s for s in all_species if s ∉ prod_species_set])
    prod_syms = sort([s for s in prod_species_set])

    # free species first, then bound species
    species = vcat(free_syms, prod_syms)
    n = length(species)
    idx = Dict(s => i for (i, s) in enumerate(species))

    # N matrix (r × n), each row follows reactants-products log-space sign convention
    N = zeros(Int, r, n)
    for i in 1:r
        for (s, coeff) in reactants[i]
            N[i, idx[s]] += coeff
        end
        for (s, coeff) in products[i]
            N[i, idx[s]] -= coeff
        end
    end

    return N, species, free_syms, prod_syms
end

function build_model(rules::Vector{String}, kd::Vector{Float64})
    r = length(rules)
    length(kd) == r || error("Length(kd) must match number of reactions")

    N, species, free_syms, prod_syms = parse_network_structure(rules)

    x_sym = Symbol.(species)
    q_sym = Symbol.("t" .* String.(free_syms))
    K_sym = Symbol.("Kd" .* string.(1:r))

    # 让 Bnc 构造函数验证合法性（n == d + r, N 线性无关等）
    model = Bnc(N=N, x_sym=x_sym, q_sym=q_sym, K_sym=K_sym)
    return model, species, free_syms, prod_syms
end

# ─── JSON helpers ───
# Convert Matrix to Vector of Vectors for proper JSON serialization
mat2vv(M::AbstractMatrix) = [collect(M[i,:]) for i in 1:size(M,1)]

json_safe_value(x::Real) = json_safe_real(x)
json_safe_value(x::AbstractString) = x
json_safe_value(x::Symbol) = String(x)
json_safe_value(x::Nothing) = nothing
json_safe_value(x::Bool) = x
json_safe_value(x) = x

function json_safe_value(data::AbstractDict)
    sanitized = Dict{Any, Any}()
    for (key, value) in data
        safe_key = key isa Symbol ? String(key) : key
        sanitized[safe_key] = json_safe_value(value)
    end
    return sanitized
end

json_safe_value(data::Tuple) = [json_safe_value(item) for item in data]
json_safe_value(data::NamedTuple) = json_safe_value(Dict(pairs(data)))
json_safe_value(data::AbstractVector) = [json_safe_value(item) for item in data]
json_safe_value(data::AbstractSet) = [json_safe_value(item) for item in collect(data)]

json_response(data; status=200) = HTTP.Response(status,
    ["Content-Type" => "application/json"],
    JSON3.write(json_safe_value(data)))

error_response(msg; status=400) = json_response(Dict("error" => msg); status)

function read_json(req)
    try
        return JSON3.read(String(req.body))
    catch e
        throw(ArgumentError("Invalid JSON: $(sprint(showerror, e))"))
    end
end

is_request_error(err) = err isa ArgumentError ||
                        err isa DomainError ||
                        err isa BoundsError ||
                        err isa KeyError

function handle_debug_logs(req)
    body = read_json(req)
    after_seq = max(0, Int(get(body, :after_seq, 0)))
    limit = clamp(Int(get(body, :limit, 300)), 1, DEBUG_LOG_LIMIT)
    client_id = debug_client_id_from_request(req)

    entries = Dict{String, Any}[]
    next_seq = 0
    total = 0
    lock(DEBUG_LOG_LOCK) do
        logs = select_debug_log_buffer(client_id)
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

    return json_response(Dict(
        "entries" => entries,
        "next_seq" => next_seq,
        "total" => total,
        "limit" => limit,
    ))
end

# ─── Vertex data extractor ───
function vertex_to_dict(model, idx)
    perm = get_perm(model, idx)
    nullity = get_nullity(model, idx)
    asymp = is_asymptotic(model, idx)
    singular = is_singular(model, idx)

    # Get species names for the permutation
    species_names = [string(model.x_sym[i]) for i in perm]

    result = Dict(
        "idx" => idx,
        "perm" => collect(perm),
        "species" => species_names,
        "nullity" => nullity,
        "asymptotic" => asymp,
        "singular" => singular,
        "x_sym" => string.(model.x_sym),
        "q_sym" => string.(model.q_sym),
        "K_sym" => string.(model.K_sym),
    )

    # Add H matrix for invertible vertices
    if nullity == 0
        H = get_H(model, idx)
        result["H"] = mat2vv(Matrix(H))
    end

    return result
end

# ─── Graph data extractor ───
function graph_to_dict(model; graph_mode::Symbol=:qk, change_qK=nothing)
    get_vertices_graph!(model; full=true)

    g, edge_label_dict, mode_label =
        if graph_mode == :qk
            (get_neighbor_graph_qK(model), get_edge_labels(model), "qK-neighbor")
        elseif graph_mode == :siso
            isnothing(change_qK) && error("change_qK is required for SISO graph mode")
            siso = SISOPaths(model, change_qK)
            siso_graph = get_neighbor_graph_qK(siso)
            edge_labels = Dict(Edge(src(e), dst(e)) => "+" * string(qK_sym(model)[siso.change_qK_idx]) for e in Graphs.edges(siso_graph))
            (siso_graph, edge_labels, "SISO")
        else
            error("Unknown graph_mode: $graph_mode")
        end

    labels = get_node_labels(model)

    # Get node sizes (which internally uses volumes)
    sizes = try
        get_node_size(model; default_node_size=44)
    catch err
        @warn "Falling back to uniform regime graph node sizes" exception=(err, catch_backtrace())
        Dict(i => 44.0 for i in 1:n_vertices(model))
    end

    # Get volumes directly - use same approach as get_node_size
    volumes_mean = try
        vals = get_volumes(model) .|> x->x.mean
        vals
    catch err
        @warn "Could not compute volumes" exception=(err, catch_backtrace())
        nothing
    end

    positions = try
        get_node_positions(model)
    catch err
        @warn "Falling back to frontend regime graph layout" exception=(err, catch_backtrace())
        nothing
    end

    nodes = []
    for i in 1:n_vertices(model)
        pos_x = isnothing(positions) ? nothing : Float64(positions[i][1])
        pos_y = isnothing(positions) ? nothing : Float64(positions[i][2])
        vol = isnothing(volumes_mean) ? nothing : Float64(volumes_mean[i])
        push!(nodes, Dict(
            "id" => i,
            "perm" => collect(get_perm(model, i)),
            "label" => labels[i],
            "size" => Float64(get(sizes, i, 44.0)),
            "volume" => vol,
            "asymptotic" => is_asymptotic(model, i),
            "singular" => is_singular(model, i),
            "nullity" => get_nullity(model, i),
            "x" => pos_x,
            "y" => pos_y,
        ))
    end

    edges = []
    for e in Graphs.edges(g)
        push!(edges, Dict(
            "source" => src(e),
            "target" => dst(e),
            "label" => get(edge_label_dict, Edge(src(e), dst(e)), ""),
        ))
    end

    return Dict(
        "graph_mode" => String(graph_mode),
        "graph_label" => mode_label,
        "change_qK" => isnothing(change_qK) ? nothing : string(change_qK),
        "nodes" => nodes,
        "edges" => edges,
    )
end

# ─── SISO data extractor ───
function siso_to_dict(model, siso)
    change_sym = string(qK_sym(model)[siso.change_qK_idx])
    paths_data = []
    for (i, path) in enumerate(siso.rgm_paths)
        perms = [collect(get_perm(model, idx)) for idx in path]
        push!(paths_data, Dict(
            "idx" => i,
            "vertex_indices" => collect(path),
            "perms" => perms,
        ))
    end

    sources_perms = [collect(get_perm(model, s)) for s in siso.sources]
    sinks_perms = [collect(get_perm(model, s)) for s in siso.sinks]

    return Dict(
        "change_qK" => change_sym,
        "change_qK_idx" => siso.change_qK_idx,
        "sources" => collect(siso.sources),
        "sinks" => collect(siso.sinks),
        "sources_perms" => sources_perms,
        "sinks_perms" => sinks_perms,
        "n_paths" => length(siso.rgm_paths),
        "paths" => paths_data,
    )
end

# ─── Polyhedron extractor (H-rep to JSON) ───
function polyhedron_to_dict(poly)
    poly === nothing && return nothing
    try
        h = MixedMatHRep(hrep(poly))
        A = mat2vv(Matrix(h.A))
        b = Vector(h.b)
        result = Dict(
            "A" => A,
            "b" => b,
            "dimension" => size(h.A, 2),
            "n_constraints" => size(h.A, 1),
            "linear_constraints" => sort!(collect(h.linset)),
        )
        # Try to get vertices
        try
            v = MixedMatVRep(vrep(poly))
            result["vertices"] = size(v.V, 1) > 0 ? mat2vv(Matrix(v.V)) : []
            result["rays"] = size(v.R, 1) > 0 ? mat2vv(Matrix(v.R)) : []
            result["ray_lineality"] = sort!(collect(v.Rlinset))
            result["n_vertices"] = size(v.V, 1)
            result["n_rays"] = size(v.R, 1)
            result["is_bounded"] = size(v.R, 1) == 0
        catch; end
        return result
    catch e
        return Dict("error" => string(e))
    end
end

# Build a conservation matrix L anchored on free species:
# L = [I_d  Z'] with N_bound * Z = -N_free so that N * L' = 0.
function derive_atomic_totals_matrix(N::Matrix{Int}, n_free::Int)
    r, n = size(N)
    d = n_free
    d > 0 || error("At least one free species is required")
    n == d + r || error("x-space closed-form requires n = d + r, got n=$n, d=$d, r=$r")

    N_free = Rational{Int}.(N[:, 1:d])
    N_bound = Rational{Int}.(N[:, d+1:end])
    size(N_bound, 1) == size(N_bound, 2) || error("Bound block of N must be square for anchored totals")
    rank(Float64.(N_bound)) == size(N_bound, 1) || error("Bound block of N is singular; cannot derive anchored totals")

    Z = -(N_bound \ N_free) # r × d
    L_rat = hcat(Matrix{Rational{Int}}(I, d, d), transpose(Z))

    residual = Rational{Int}.(N) * transpose(L_rat)
    all(iszero, residual) || error("Failed to derive valid conservation matrix (N * L' != 0)")

    L = Float64.(L_rat)
    L[abs.(L) .< 1e-12] .= 0.0
    return L
end

function compute_rop_cloud_xspace(rules::Vector{String}, n_samples::Int, logx_min::Float64, logx_max::Float64;
    target_species::Union{Nothing,Symbol}=nothing)
    logx_max > logx_min || error("logx_max must be greater than logx_min")

    N, species, free_syms, prod_syms = parse_network_structure(rules)
    L = derive_atomic_totals_matrix(N, length(free_syms))

    r, n = size(N)
    d = size(L, 1)
    n == d + r || error("Internal error: expected n=d+r")

    target_sym = if isnothing(target_species)
        !isempty(prod_syms) ? prod_syms[1] : species[end]
    else
        target_species
    end
    target_idx = findfirst(==(target_sym), species)
    target_idx === nothing && error("target_species '$target_sym' not found in species: $(join(string.(species), ", "))")

    reaction_orders = Matrix{Float64}(undef, n_samples, d)
    target_values = Vector{Float64}(undef, n_samples)

    Nf = Float64.(N)
    A = Matrix{Float64}(undef, n, n)
    A[d+1:end, :] .= Nf
    e_target = zeros(Float64, n)
    e_target[target_idx] = 1.0

    accepted = 0
    attempts = 0
    max_attempts = max(10 * n_samples, n_samples + 1000)

    while accepted < n_samples && attempts < max_attempts
        attempts += 1

        logx = logx_min .+ (logx_max - logx_min) .* rand(n)
        x = exp10.(logx)
        t = L * x
        if any(!isfinite, t) || any(t .<= 0.0)
            continue
        end

        @views A[1:d, :] .= (L .* transpose(x)) ./ t
        y = try
            A' \ e_target
        catch err
            if err isa SingularException || err isa LinearAlgebra.LAPACKException
                continue
            else
                rethrow(err)
            end
        end

        if any(!isfinite, y)
            continue
        end

        accepted += 1
        @views reaction_orders[accepted, :] .= y[1:d]
        target_values[accepted] = x[target_idx]
    end

    accepted == n_samples || error("Only accepted $accepted / $n_samples samples (attempts=$attempts)")

    q_sym = Symbol.("t" .* String.(free_syms))
    return reaction_orders, target_values, q_sym, d, species[target_idx]
end

# ─── ROP point cloud computation ───
function compute_rop_cloud(model, kd, prod_syms_sym, n_samples, span)
    logK = log10.(kd)
    logq_min = minimum(logK) - span
    logq_max = maximum(logK) + span

    d = model.d
    prod_idx = [locate_sym_x(model, s) for s in prod_syms_sym]

    # Allocate output
    reaction_orders = Matrix{Float64}(undef, n_samples, d)
    fret_values = Vector{Float64}(undef, n_samples)

    for k in 1:n_samples
        logq = logq_min .+ (logq_max - logq_min) .* rand(d)
        logqK = vcat(logq, logK)
        x = qK2x(model, logqK; input_logspace=true, output_logspace=false)
        J = ∂logx_∂logqK(model; qK=logqK, input_logspace=true)

        # Weighted reaction orders for FRET proxy
        w = x[prod_idx] ./ sum(x[prod_idx])
        ro = vec(w' * J[prod_idx, 1:d])
        reaction_orders[k, :] = ro
        fret_values[k] = sum(x[prod_idx])
    end

    return reaction_orders, fret_values
end

# ─── SISO trajectory computation ───
function compute_siso_trajectory(model, siso, path_idx; npoints=500, start_val=-6, stop_val=6)
    path_idx_int = path_idx
    poly = get_polyhedron(siso, path_idx_int)
    params = get_one_inner_point(poly; rand_line=false, rand_ray=false, extend=4)

    change_idx = siso.change_qK_idx

    start_logqK = copy(params) |> x -> insert!(x, change_idx, start_val)
    end_logqK = copy(params) |> x -> insert!(x, change_idx, stop_val)

    t_ode, logx_traj = x_traj_with_qK_change(model, start_logqK, end_logqK;
        input_logspace=true, output_logspace=true, npoints=npoints, ensure_manifold=true)

    # t_ode is in [0,1], map to actual logqK change values
    change_values = start_val .+ t_ode .* (stop_val - start_val)

    regimes = [assign_vertex_x(model, lx; input_logspace=true, return_idx=true) for lx in logx_traj]

    # Convert to matrix
    logx_mat = reduce(hcat, logx_traj)'  # npoints × n

    return Dict(
        "change_values" => collect(change_values),
        "logx" => mat2vv(Matrix(logx_mat)),
        "regimes" => regimes,
        "x_sym" => string.(model.x_sym),
        "change_sym" => string(qK_sym(model)[change_idx]),
        "parameters" => params,
    )
end

json_safe_real(x::Real) = if isnan(x)
    "NaN"
elseif isinf(x)
    signbit(x) ? "-Inf" : "Inf"
else
    Float64(x)
end

json_safe_profile(profile::AbstractVector{<:Real}) = [json_safe_real(x) for x in profile]
json_safe_profile(profile::AbstractVector{<:AbstractVector{<:Real}}) = [json_safe_profile(coords) for coords in profile]

volume_to_dict(vol) = isnothing(vol) ? nothing : Dict(
    "mean" => vol.mean,
    "var" => vol.var,
    "std" => sqrt(vol.var),
    "rel_error" => vol.mean == 0 ? nothing : sqrt(vol.var) / vol.mean,
)

function behavior_scope_note(path_scope::Symbol, min_volume_mean::Float64)
    if path_scope == :all
        return "Including every graph path, even when the corresponding path polyhedron is empty. Use this only for graph-level overviews."
    elseif path_scope == :feasible
        return "Including only paths with non-empty path polyhedra. Paths excluded here are graph-theoretic paths that have no common parameter region after all path constraints are intersected."
    else
        return "Including only feasible paths whose estimated volume mean is at least $(min_volume_mean). This is a robustness filter on top of feasibility."
    end
end

function behavior_result_to_dict(model, siso, result)
    path_dicts = Vector{Dict{String,Any}}(undef, length(result.path_records))
    for rec in result.path_records
        path_dicts[rec.path_idx] = Dict(
            "path_idx" => rec.path_idx,
            "vertex_indices" => collect(rec.vertex_indices),
            "perms" => [collect(get_perm(model, idx)) for idx in rec.vertex_indices],
            "exact_profile" => json_safe_profile(rec.exact_profile),
            "exact_label" => rec.exact_label,
            "motif_profile" => collect(rec.motif_profile),
            "motif_label" => rec.motif_label,
            "feasible" => rec.feasible,
            "feasibility_checked" => rec.feasibility_checked,
            "included" => rec.included,
            "exclusion_reason" => rec.exclusion_reason,
            "volume" => volume_to_dict(rec.volume),
        )
    end

    exact_families = map(result.exact_families) do family
        Dict(
            "family_idx" => family.family_idx,
            "exact_profile" => json_safe_profile(family.exact_profile),
            "exact_label" => family.exact_label,
            "motif_profile" => collect(family.motif_profile),
            "motif_label" => family.motif_label,
            "path_indices" => collect(family.path_indices),
            "n_paths" => family.n_paths,
            "total_volume" => volume_to_dict(family.total_volume),
            "representative_path_idx" => family.representative_path_idx,
            "representative_volume" => volume_to_dict(family.representative_volume),
        )
    end

    motif_families = map(result.motif_families) do family
        Dict(
            "family_idx" => family.family_idx,
            "motif_profile" => collect(family.motif_profile),
            "motif_label" => family.motif_label,
            "path_indices" => collect(family.path_indices),
            "exact_family_indices" => collect(family.exact_family_indices),
            "n_paths" => family.n_paths,
            "total_volume" => volume_to_dict(family.total_volume),
            "representative_path_idx" => family.representative_path_idx,
            "representative_volume" => volume_to_dict(family.representative_volume),
        )
    end

    change_qK_indices = Int[Int(idx) for idx in result.change_qK_indices]
    change_qK_signs = Int[Int(sign) for sign in result.change_qK_signs]
    change_qK_symbols = [string(qK_sym(model)[idx]) for idx in change_qK_indices]
    change_qK = result.change_qK_idx === nothing ? nothing : string(qK_sym(model)[result.change_qK_idx])

    return Dict(
        "change_kind" => string(result.change_kind),
        "change_label" => string(result.change_label),
        "change_qK" => change_qK,
        "change_qK_idx" => result.change_qK_idx,
        "change_qK_indices" => change_qK_indices,
        "change_qK_signs" => change_qK_signs,
        "change_qK_symbols" => change_qK_symbols,
        "observe_x" => string(x_sym(model)[result.observe_x_idx]),
        "observe_x_idx" => result.observe_x_idx,
        "path_scope" => string(result.path_scope),
        "scope_note" => behavior_scope_note(result.path_scope, result.min_volume_mean),
        "min_volume_mean" => result.min_volume_mean,
        "deduplicate" => result.deduplicate,
        "keep_singular" => result.keep_singular,
        "keep_nonasymptotic" => result.keep_nonasymptotic,
        "compute_volume" => result.compute_volume,
        "feasibility_mode" => string(result.feasibility_mode),
        "total_paths" => result.total_paths,
        "feasible_paths" => result.feasible_paths,
        "included_paths" => result.included_paths,
        "excluded_paths" => result.excluded_paths,
        "exclusion_counts" => Dict(string(k) => v for (k, v) in result.exclusion_counts),
        "paths" => path_dicts,
        "exact_families" => exact_families,
        "motif_families" => motif_families,
        "sources" => collect(siso.sources),
        "sinks" => collect(siso.sinks),
    )
end

include(joinpath(@__DIR__, "atlas.jl"))
include(joinpath(@__DIR__, "behavior_program_codec.jl"))
include(joinpath(@__DIR__, "atlas_sqlite.jl"))
include(joinpath(@__DIR__, "inverse_design_v2.jl"))

# ─── API Route Handlers ───

function handle_build_atlas(req)
    body = read_json(req)
    return json_response(build_behavior_atlas_from_spec(body))
end

function handle_query_atlas(req)
    body = read_json(req)
    return json_response(query_behavior_atlas_from_spec(body))
end

function handle_build_atlas_library(req)
    body = read_json(req)
    return json_response(build_atlas_library_from_spec(body))
end

function handle_merge_atlas_library(req)
    body = read_json(req)
    return json_response(merge_atlas_library_from_spec(body))
end

function handle_run_inverse_design(req)
    body = read_json(req)
    return json_response(run_inverse_design_from_spec(body))
end

function handle_build_model(req)
    body = read_json(req)
    rules = String.(body[:reactions])
    kd = Float64.(body[:kd])
    sid = get(body, :session_id, string(rand(UInt32), base=16))

    # Validate Kd values
    if any(x -> x <= 0, kd)
        return error_response("All Kd values must be positive (> 0)"; status=400)
    end

    model, species, free_syms, prod_syms = build_model(rules, kd)
    set_session(sid, Dict(
        "model" => model,
        "species" => species,
        "free_syms" => free_syms,
        "prod_syms" => prod_syms,
        "kd" => kd,
        "rules" => rules,
    ))

    return json_response(Dict(
        "session_id" => sid,
        "n" => model.n, "d" => model.d, "r" => model.r,
        "species" => string.(species),
        "free_species" => string.(free_syms),
        "product_species" => string.(prod_syms),
        "x_sym" => string.(model.x_sym),
        "q_sym" => string.(model.q_sym),
        "K_sym" => string.(model.K_sym),
        "N" => mat2vv(Matrix(model.N)),
        "L" => mat2vv(Matrix(model.L)),
    ))
end

function handle_find_vertices(req)
    body = read_json(req)
    sid = String(body[:session_id])
    sess = get_session(sid)
    sess === nothing && return error_response("Invalid session_id"; status=404)

    model = sess["model"]

    find_all_vertices!(model)

    vertices = []
    for i in 1:n_vertices(model)
        perm = get_perm(model, i)
        species_names = [string(model.x_sym[j]) for j in perm]
        push!(vertices, Dict(
            "idx" => i,
            "perm" => collect(perm),
            "species" => species_names,
            "asymptotic" => is_asymptotic(model, i),
            "singular" => is_singular(model, i),
            "nullity" => get_nullity(model, i),
        ))
    end

    return json_response(Dict(
        "n_vertices" => n_vertices(model),
        "vertices" => vertices,
    ))
end

function handle_build_graph(req)
    body = read_json(req)
    sid = String(body[:session_id])
    sess = get_session(sid)
    sess === nothing && return error_response("Invalid session_id"; status=404)

    model = sess["model"]
    graph_mode = Symbol(String(get(body, :graph_mode, "qk")))
    change_qK = haskey(body, :change_qK) ? Symbol(String(body[:change_qK])) : nothing

    data = graph_to_dict(model; graph_mode=graph_mode, change_qK=change_qK)
    return json_response(data)
end

function handle_siso_paths(req)
    body = read_json(req)
    sid = String(body[:session_id])
    sess = get_session(sid)
    sess === nothing && return error_response("Invalid session_id"; status=404)

    model = sess["model"]
    change_qK = Symbol(body[:change_qK])

    siso = SISOPaths(model, change_qK)
    sess["siso_$(body[:change_qK])"] = siso

    data = siso_to_dict(model, siso)
    return json_response(data)
end

function handle_siso_polyhedra(req)
    body = read_json(req)
    sid = String(body[:session_id])
    sess = get_session(sid)
    sess === nothing && return error_response("Invalid session_id"; status=404)

    model = sess["model"]
    change_key = "siso_$(body[:change_qK])"
    siso = get(sess, change_key, nothing)
    siso === nothing && return error_response("SISO paths not computed for this qK coordinate"; status=404)

    path_indices = haskey(body, :path_indices) ? Int.(body[:path_indices]) : collect(1:length(siso.rgm_paths))
    # Limit to avoid huge computation
    path_indices = path_indices[1:min(length(path_indices), 50)]

    polys = get_polyhedra(siso, path_indices)
    poly_data = []
    for (i, pi) in enumerate(path_indices)
        pd = polyhedron_to_dict(polys[i])
        pd["path_idx"] = pi
        pd["path"] = collect(siso.rgm_paths[pi])
        pd["perms"] = [collect(get_perm(model, idx)) for idx in siso.rgm_paths[pi]]
        push!(poly_data, pd)
    end

    return json_response(Dict(
        "change_qK" => string(qK_sym(model)[siso.change_qK_idx]),
        "qk_symbols" => string.(qK_sym(siso)),
        "polyhedra" => poly_data,
    ))
end

function handle_siso_path_condition(req)
    body = read_json(req)
    sid = String(body[:session_id])
    sess = get_session(sid)
    sess === nothing && return error_response("Invalid session_id"; status=404)

    model = sess["model"]
    change_key = "siso_$(body[:change_qK])"
    siso = get(sess, change_key, nothing)
    siso === nothing && return error_response("SISO paths not computed for this qK coordinate"; status=404)

    path_idx = Int(body[:path_idx])
    if path_idx < 1 || path_idx > length(siso.rgm_paths)
        return error_response("path_idx out of range (1-$(length(siso.rgm_paths)))"; status=400)
    end

    conditions = BindingAndCatalysis.show_condition_path(siso, path_idx)
    return json_response(Dict(
        "change_qK" => string(qK_sym(model)[siso.change_qK_idx]),
        "qk_symbols" => string.(qK_sym(siso)),
        "path_idx" => path_idx,
        "path" => collect(siso.rgm_paths[path_idx]),
        "perms" => [collect(get_perm(model, idx)) for idx in siso.rgm_paths[path_idx]],
        "conditions" => [sprint(show, condition) for condition in conditions],
    ))
end

function handle_siso_trajectory(req)
    body = read_json(req)
    sid = String(body[:session_id])
    sess = get_session(sid)
    sess === nothing && return error_response("Invalid session_id"; status=404)

    model = sess["model"]
    change_key = "siso_$(body[:change_qK])"
    siso = get(sess, change_key, nothing)
    siso === nothing && return error_response("SISO paths not computed for this qK coordinate"; status=404)

    path_idx = Int(body[:path_idx])

    # Validate path_idx
    if path_idx < 1 || path_idx > length(siso.rgm_paths)
        return error_response("path_idx out of range (1-$(length(siso.rgm_paths)))"; status=400)
    end

    npoints = clamp(get(body, :npoints, 500), 10, 5000)
    start_val = get(body, :start, -6)
    stop_val = get(body, :stop, 6)

    data = compute_siso_trajectory(model, siso, path_idx;
        npoints=npoints, start_val=start_val, stop_val=stop_val)
    return json_response(data)
end

function handle_behavior_families(req)
    body = read_json(req)
    sid = String(body[:session_id])
    sess = get_session(sid)
    sess === nothing && return error_response("Invalid session_id"; status=404)

    model = sess["model"]
    change_qK = Symbol(body[:change_qK])
    observe_x = haskey(body, :observe_x) ? Symbol(body[:observe_x]) : error("observe_x is required")
    path_scope = Symbol(get(body, :path_scope, "feasible"))
    min_volume_mean = Float64(get(body, :min_volume_mean, 0.0))
    deduplicate = Bool(get(body, :deduplicate, true))
    keep_singular = Bool(get(body, :keep_singular, true))
    keep_nonasymptotic = Bool(get(body, :keep_nonasymptotic, false))
    compute_volume = Bool(get(body, :compute_volume, true))

    change_key = "siso_$(body[:change_qK])"
    siso = get(sess, change_key, nothing)
    if siso === nothing
        siso = SISOPaths(model, change_qK)
        sess[change_key] = siso
    end

    result = get_behavior_families(
        siso;
        observe_x=observe_x,
        path_scope=path_scope,
        min_volume_mean=min_volume_mean,
        deduplicate=deduplicate,
        keep_singular=keep_singular,
        keep_nonasymptotic=keep_nonasymptotic,
        compute_volume=compute_volume,
    )

    return json_response(behavior_result_to_dict(model, siso, result))
end

function handle_rop_cloud(req)
    body = read_json(req)
    mode = lowercase(String(get(body, :sampling_mode, "qk")))

    n_samples = clamp(Int(get(body, :n_samples, 10000)), 100, 100000)

    if mode == "x_space"
        rules = if haskey(body, :reactions)
            String.(body[:reactions])
        else
            haskey(body, :session_id) || return error_response("x-space mode requires reactions or session_id"; status=400)
            sid = String(body[:session_id])
            sess = get_session(sid)
            sess === nothing && return error_response("Invalid session_id"; status=404)
            String.(sess["rules"])
        end

        isempty(rules) && return error_response("At least one reaction is required"; status=400)

        logx_min = Float64(get(body, :logx_min, -6.0))
        logx_max = Float64(get(body, :logx_max, 6.0))
        logx_min = clamp(logx_min, -20.0, 20.0)
        logx_max = clamp(logx_max, -20.0, 20.0)
        logx_max > logx_min || return error_response("logx_max must be greater than logx_min"; status=400)

        target_species = if haskey(body, :target_species)
            raw = strip(String(body[:target_species]))
            isempty(raw) ? nothing : Symbol(raw)
        else
            nothing
        end

        ro, target_vals, q_sym, d, target_sym = compute_rop_cloud_xspace(
            rules, n_samples, logx_min, logx_max; target_species=target_species
        )

        return json_response(Dict(
            "reaction_orders" => mat2vv(ro),
            "fret_values" => target_vals, # kept for plotting color compatibility
            "q_sym" => string.(q_sym),
            "d" => d,
            "sampling_mode" => mode,
            "target_species" => string(target_sym),
        ))
    elseif mode == "qk"
        haskey(body, :session_id) || return error_response("qK mode requires session_id"; status=400)
        sid = String(body[:session_id])
        sess = get_session(sid)
        sess === nothing && return error_response("Invalid session_id"; status=404)

        model = sess["model"]
        kd = sess["kd"]
        prod_syms = Symbol.(sess["prod_syms"])

        span = clamp(Int(get(body, :span, 6)), 1, 20)
        ro, fret = compute_rop_cloud(model, kd, prod_syms, n_samples, span)

        return json_response(Dict(
            "reaction_orders" => mat2vv(ro),
            "fret_values" => fret,
            "q_sym" => string.(model.q_sym),
            "d" => model.d,
            "sampling_mode" => mode,
        ))
    else
        return error_response("Unsupported sampling_mode '$mode' (use 'qk' or 'x_space')"; status=400)
    end
end

function handle_vertex_detail(req)
    body = read_json(req)
    sid = String(body[:session_id])
    sess = get_session(sid)
    sess === nothing && return error_response("Invalid session_id"; status=404)

    model = sess["model"]
    idx = Int(body[:vertex_idx])

    if idx < 1 || idx > n_vertices(model)
        return error_response("vertex_idx out of range (1-$(n_vertices(model)))"; status=400)
    end

    data = vertex_to_dict(model, idx)
    return json_response(data)
end

# ─── FRET heatmap computation (2D only) ───
function handle_fret_heatmap(req)
    body = read_json(req)
    sid = String(body[:session_id])
    sess = get_session(sid)
    sess === nothing && return error_response("Invalid session_id"; status=404)

    model = sess["model"]
    kd = sess["kd"]
    prod_syms = Symbol.(sess["prod_syms"])

    n_grid = clamp(get(body, :n_grid, 80), 20, 300)
    logq_min = get(body, :logq_min, -6)
    logq_max = get(body, :logq_max, 6)
    logK = log10.(kd)

    model.d == 2 || return error_response("FRET heatmap only supports d=2"; status=400)

    prod_idx = [locate_sym_x(model, s) for s in prod_syms]
    logq1 = range(logq_min, logq_max, length=n_grid)
    logq2 = range(logq_min, logq_max, length=n_grid)

    fret = zeros(Float64, n_grid, n_grid)
    regime = zeros(Int, n_grid, n_grid)

    for (i, lq1) in enumerate(logq1)
        for (j, lq2) in enumerate(logq2)
            logqK = vcat([lq1, lq2], logK)
            x = qK2x(model, logqK; input_logspace=true, output_logspace=false)
            fret[i, j] = sum(x[prod_idx])
            regime[i, j] = assign_vertex_qK(model, logqK; input_logspace=true, return_idx=true)
        end
    end

    bounds = find_bounds(regime)

    return json_response(Dict(
        "logq1" => collect(logq1),
        "logq2" => collect(logq2),
        "fret" => mat2vv(fret),
        "regime" => mat2vv(regime),
        "bounds" => mat2vv(Float64.(bounds)),
        "q_sym" => string.(model.q_sym),
    ))
end

# ─── New API handlers for parameter scanning ───
function handle_parameter_scan_1d(req)
    body = read_json(req)
    sid = String(body[:session_id])
    sess = get_session(sid)
    sess === nothing && return error_response("Invalid session_id"; status=404)

    model = sess["model"]

    # Parse parameters
    param_symbol = Symbol(body[:param_symbol])
    param_idx = locate_sym_qK(model, param_symbol)
    param_idx === nothing && return error_response("Unknown parameter: $param_symbol"; status=400)

    param_min = Float64(get(body, :param_min, -6.0))
    param_max = Float64(get(body, :param_max, 6.0))
    n_points = clamp(Int(get(body, :n_points, 200)), 10, 1000)

    # Parse output expressions
    output_exprs_raw = body[:output_exprs]
    # Normalize to array if single string provided
    output_exprs = if output_exprs_raw isa String
        [String(output_exprs_raw)]
    else
        String.(output_exprs_raw)
    end
    isempty(output_exprs) && return error_response("At least one output expression required"; status=400)

    output_coeffs = Vector{Vector{Float64}}()
    for expr in output_exprs
        try
            coeffs = parse_linear_combination(model, expr)
            push!(output_coeffs, coeffs)
        catch e
            return error_response("Invalid expression '$expr': $(sprint(showerror, e))"; status=400)
        end
    end

    # Fixed parameters (all qK except the scanned one)
    fixed_qK = if haskey(body, :fixed_qK)
        Float64.(body[:fixed_qK])
    else
        # Default: all zeros (log-space), meaning all parameters = 1
        zeros(Float64, model.n)
    end

    # Remove scanned parameter from fixed_qK
    fixed_params = deleteat!(copy(fixed_qK), param_idx)

    # Scan
    param_range = range(param_min, param_max, length=n_points) |> collect
    param_vals, output_traj, regimes = scan_parameter_1d(
        model, param_idx, param_range, output_coeffs, fixed_params;
        input_logspace=true, output_logspace=true
    )

    return json_response(Dict(
        "param_symbol" => string(param_symbol),
        "param_values" => param_vals,
        "output_exprs" => output_exprs,
        "output_traj" => mat2vv(output_traj),
        "regimes" => regimes,
        "x_sym" => string.(model.x_sym),
    ))
end

function handle_parameter_scan_2d(req)
    body = read_json(req)
    sid = String(body[:session_id])
    sess = get_session(sid)
    sess === nothing && return error_response("Invalid session_id"; status=404)

    model = sess["model"]

    # Parse parameters
    param1_symbol = Symbol(body[:param1_symbol])
    param2_symbol = Symbol(body[:param2_symbol])
    param1_idx = locate_sym_qK(model, param1_symbol)
    param2_idx = locate_sym_qK(model, param2_symbol)
    param1_idx === nothing && return error_response("Unknown parameter: $param1_symbol"; status=400)
    param2_idx === nothing && return error_response("Unknown parameter: $param2_symbol"; status=400)
    param1_idx == param2_idx && return error_response("Parameters must be different"; status=400)

    param1_min = Float64(get(body, :param1_min, -6.0))
    param1_max = Float64(get(body, :param1_max, 6.0))
    param2_min = Float64(get(body, :param2_min, -6.0))
    param2_max = Float64(get(body, :param2_max, 6.0))
    n_grid = clamp(Int(get(body, :n_grid, 80)), 20, 200)

    # Parse output expression (single output for heatmap)
    output_expr = String(body[:output_expr])
    output_coeffs = try
        parse_linear_combination(model, output_expr)
    catch e
        return error_response("Invalid expression '$output_expr': $(sprint(showerror, e))"; status=400)
    end

    # Fixed parameters
    fixed_qK = if haskey(body, :fixed_qK)
        Float64.(body[:fixed_qK])
    else
        zeros(Float64, model.n)
    end

    # Remove both scanned parameters (in descending order to avoid index shifts)
    indices_to_remove = sort([param1_idx, param2_idx], rev=true)
    fixed_params = copy(fixed_qK)
    for idx in indices_to_remove
        deleteat!(fixed_params, idx)
    end

    # Scan
    param1_range = range(param1_min, param1_max, length=n_grid) |> collect
    param2_range = range(param2_min, param2_max, length=n_grid) |> collect

    param1_vals, param2_vals, output_grid, regime_grid = scan_parameter_2d(
        model, param1_idx, param2_idx, param1_range, param2_range,
        output_coeffs, fixed_params;
        input_logspace=true, output_logspace=true
    )

    return json_response(Dict(
        "param1_symbol" => string(param1_symbol),
        "param2_symbol" => string(param2_symbol),
        "param1_values" => param1_vals,
        "param2_values" => param2_vals,
        "output_expr" => output_expr,
        "output_grid" => mat2vv(output_grid),
        "regime_grid" => mat2vv(regime_grid),
    ))
end

function _atlas_landscape_model_from_spec(body)
    if _raw_haskey(body, :session_id)
        sid = String(_raw_get(body, :session_id, ""))
        sess = get_session(sid)
        sess === nothing && error("Invalid session_id")
        return (
            model=sess["model"],
            rules=String.(sess["rules"]),
            kd=Float64.(sess["kd"]),
        )
    end

    _raw_haskey(body, :reactions) || error("Landscape scan requires `session_id` or `reactions`.")
    rules = String.(collect(_raw_get(body, :reactions, String[])))
    isempty(rules) && error("At least one reaction is required for a landscape scan.")
    kd = if _raw_haskey(body, :kd)
        Float64.(collect(_raw_get(body, :kd, Float64[])))
    else
        ones(Float64, length(rules))
    end
    length(kd) == length(rules) || error("Length of `kd` must match `reactions`.")
    any(x -> x <= 0, kd) && error("All Kd values must be positive (> 0).")
    model, _, _, _ = build_model(rules, kd)
    return (model=model, rules=rules, kd=kd)
end

function _atlas_landscape_param_options(model)
    total_syms = string.(q_sym(model))
    all_syms = string.(qK_sym(model))
    extras = [sym for sym in all_syms if sym ∉ total_syms]
    return vcat(total_syms, extras)
end

function _atlas_landscape_pick_params(model, body)
    options = _atlas_landscape_param_options(model)
    length(options) >= 2 || error("2D landscape requires at least two q/K coordinates.")

    preferred = if _raw_haskey(body, :preferred_param_symbols)
        [String(sym) for sym in collect(_raw_get(body, :preferred_param_symbols, String[])) if !isempty(strip(String(sym)))]
    else
        String[]
    end

    selected = String[]
    seen = Set{String}()
    for sym in preferred
        sym in options || continue
        sym in seen && continue
        push!(selected, sym)
        push!(seen, sym)
        length(selected) == 2 && break
    end

    if _raw_haskey(body, :param1_symbol)
        sym = String(_raw_get(body, :param1_symbol, ""))
        sym in options || error("Unknown parameter: $(sym)")
        empty!(selected)
        empty!(seen)
        push!(selected, sym)
        push!(seen, sym)
    end
    if _raw_haskey(body, :param2_symbol)
        sym = String(_raw_get(body, :param2_symbol, ""))
        sym in options || error("Unknown parameter: $(sym)")
        sym in seen && error("Parameters must be different.")
        push!(selected, sym)
        push!(seen, sym)
    end

    for sym in options
        sym in seen && continue
        push!(selected, sym)
        push!(seen, sym)
        length(selected) == 2 && break
    end

    length(selected) >= 2 || error("Could not resolve two parameters for the landscape scan.")
    return selected[1], selected[2], options
end

function atlas_landscape_2d_from_spec(body)
    ctx = _atlas_landscape_model_from_spec(body)
    model = ctx.model

    param1_symbol, param2_symbol, param_symbol_options = _atlas_landscape_pick_params(model, body)
    param1_idx = locate_sym_qK(model, Symbol(param1_symbol))
    param2_idx = locate_sym_qK(model, Symbol(param2_symbol))
    (param1_idx === nothing || param2_idx === nothing) && error("Could not locate requested parameters in the model.")
    param1_idx == param2_idx && error("Parameters must be different.")

    output_symbol_options = string.(model.x_sym)
    default_output = isempty(output_symbol_options) ? "" : output_symbol_options[1]
    output_expr = if _raw_haskey(body, :output_expr)
        String(_raw_get(body, :output_expr, default_output))
    elseif _raw_haskey(body, :output_symbol)
        String(_raw_get(body, :output_symbol, default_output))
    else
        default_output
    end
    isempty(strip(output_expr)) && error("Landscape scan requires an output expression.")
    output_coeffs = parse_linear_combination(model, output_expr)

    param1_min = Float64(_raw_get(body, :param1_min, -4.0))
    param1_max = Float64(_raw_get(body, :param1_max, 4.0))
    param2_min = Float64(_raw_get(body, :param2_min, -4.0))
    param2_max = Float64(_raw_get(body, :param2_max, 4.0))
    param1_max > param1_min || error("param1_max must be greater than param1_min.")
    param2_max > param2_min || error("param2_max must be greater than param2_min.")
    n_grid = clamp(Int(_raw_get(body, :n_grid, 72)), 20, 160)

    fixed_qK = if _raw_haskey(body, :fixed_qK)
        Float64.(collect(_raw_get(body, :fixed_qK, Float64[])))
    else
        zeros(Float64, model.n)
    end
    length(fixed_qK) == model.n || error("Length of `fixed_qK` must equal the full q/K dimension ($(model.n)).")

    indices_to_remove = sort([param1_idx, param2_idx], rev=true)
    fixed_params = copy(fixed_qK)
    for idx in indices_to_remove
        deleteat!(fixed_params, idx)
    end

    param1_range = collect(range(param1_min, param1_max, length=n_grid))
    param2_range = collect(range(param2_min, param2_max, length=n_grid))
    param1_vals, param2_vals, output_grid, regime_grid = scan_parameter_2d(
        model,
        param1_idx,
        param2_idx,
        param1_range,
        param2_range,
        output_coeffs,
        fixed_params;
        input_logspace=true,
        output_logspace=true,
    )
    bounds = find_bounds(regime_grid)

    return Dict(
        "param1_symbol" => param1_symbol,
        "param2_symbol" => param2_symbol,
        "param1_values" => param1_vals,
        "param2_values" => param2_vals,
        "param_symbol_options" => param_symbol_options,
        "output_expr" => output_expr,
        "output_symbol_options" => output_symbol_options,
        "output_grid" => mat2vv(output_grid),
        "regime_grid" => mat2vv(regime_grid),
        "bounds" => mat2vv(Float64.(bounds)),
        "fixed_qK" => collect(fixed_qK),
        "q_sym" => string.(q_sym(model)),
        "K_sym" => string.(K_sym(model)),
        "x_sym" => string.(model.x_sym),
        "rules" => ctx.rules,
        "kd" => collect(ctx.kd),
        "n_grid" => n_grid,
    )
end

function handle_atlas_landscape_2d(req)
    body = read_json(req)
    try
        return json_response(atlas_landscape_2d_from_spec(body))
    catch e
        return error_response(sprint(showerror, e); status=400)
    end
end

function handle_rop_polyhedron(req)
    body = read_json(req)
    sid = String(body[:session_id])
    sess = get_session(sid)
    sess === nothing && return error_response("Invalid session_id"; status=404)

    model = sess["model"]

    if haskey(body, :pairs)
        pairs_in = body[:pairs]
        length(pairs_in) >= 2 || return error_response("At least two ROP axes are required"; status=400)

        pairs = Tuple{Symbol, Symbol}[]
        for pair in pairs_in
            x_symbol = Symbol(pair[:x_symbol])
            qk_symbol = Symbol(pair[:qk_symbol])
            locate_sym_x(model, x_symbol) === nothing && return error_response("Unknown species: $x_symbol"; status=400)
            locate_sym_qK(model, qk_symbol) === nothing && return error_response("Unknown qK symbol: $qk_symbol"; status=400)
            push!(pairs, (x_symbol, qk_symbol))
        end

        add_inner_points = Bool(get(body, :add_inner_points, true))
        npoints = clamp(Int(get(body, :npoints, 5000)), 0, 100000)
        singular_extends = Float64(get(body, :singular_extends, 2.0))

        rop_data = try
            get_ROP_plot_data(
                model,
                pairs;
                add_inner_points=add_inner_points,
                npoints=npoints,
                singular_extends=singular_extends,
            )
        catch e
            return error_response("Failed to compute ROP geometry: $(sprint(showerror, e))"; status=500)
        end

        point_json = [
            Dict(
                "coords" => collect(point.coords),
                "vertex_idx" => point.vertex_idx,
                "perm" => point.perm,
                "point_type" => String(point.point_type),
            ) for point in rop_data.points
        ]
        pair_json = [
            Dict(
                "x_symbol" => pair.x_symbol,
                "qk_symbol" => pair.qK_symbol,
                "label" => pair.label,
            ) for pair in rop_data.pairs
        ]
        edge_json(edges, include_to_idx::Bool=true) = [
            include_to_idx ?
            Dict(
                "from" => collect(edge.from),
                "to" => collect(edge.to),
                "from_idx" => edge.from_idx,
                "to_idx" => edge.to_idx,
            ) :
            Dict(
                "from" => collect(edge.from),
                "to" => collect(edge.to),
                "from_idx" => edge.from_idx,
                "singular_idx" => edge.singular_idx,
            ) for edge in edges
        ]

        return json_response(Dict(
            "dimension" => rop_data.dimension,
            "pairs" => pair_json,
            "axis_labels" => collect(rop_data.axis_labels),
            "add_inner_points" => add_inner_points,
            "npoints" => npoints,
            "singular_extends" => singular_extends,
            "points" => point_json,
            "direct_edges" => edge_json(rop_data.direct_edges, true),
            "indirect_edges" => edge_json(rop_data.indirect_edges, true),
            "direct_rays" => edge_json(rop_data.direct_rays, false),
            "indirect_rays" => edge_json(rop_data.indirect_rays, false),
            "inner_points" => [collect(point) for point in rop_data.inner_points],
        ))
    end

    haskey(body, :output_expr) || return error_response(
        "ROP request must include either `pairs` for draw_ROP axes or legacy `output_expr` parameters";
        status=400,
    )

    # Parse output expression
    output_expr = String(body[:output_expr])
    output_coeffs = try
        parse_linear_combination(model, output_expr)
    catch e
        return error_response("Invalid expression '$output_expr': $(sprint(showerror, e))"; status=400)
    end

    # Parse parameters
    param1_symbol = Symbol(body[:param1_symbol])
    param2_symbol = Symbol(body[:param2_symbol])
    param1_idx = locate_sym_qK(model, param1_symbol)
    param2_idx = locate_sym_qK(model, param2_symbol)
    param1_idx === nothing && return error_response("Unknown parameter: $param1_symbol"; status=400)
    param2_idx === nothing && return error_response("Unknown parameter: $param2_symbol"; status=400)
    param1_idx == param2_idx && return error_response("Parameters must be different"; status=400)

    asymptotic_only = get(body, :asymptotic_only, true)
    max_vertices = clamp(Int(get(body, :max_vertices, 1000)), 10, 5000)

    # Compute polyhedron
    poly_data = try
        compute_rop_polyhedron(model, output_coeffs, param1_idx, param2_idx;
                               asymptotic_only=asymptotic_only, max_vertices=max_vertices)
    catch e
        return error_response("Failed to compute polyhedron: $(sprint(showerror, e))"; status=500)
    end

    # Format vertices for JSON
    vertices_json = []
    for (ro1, ro2, idx, nullity, perm) in poly_data["vertices"]
        push!(vertices_json, Dict(
            "ro1" => ro1,
            "ro2" => ro2,
            "idx" => idx,
            "nullity" => nullity,
            "perm" => perm,
        ))
    end

    return json_response(Dict(
        "output_expr" => output_expr,
        "param1_symbol" => string(param1_symbol),
        "param2_symbol" => string(param2_symbol),
        "vertices" => vertices_json,
        "edges" => poly_data["edges"],
    ))
end

function _is_subpath(path::AbstractString, root::AbstractString)
    norm_path = normpath(path)
    norm_root = normpath(root)
    norm_path == norm_root && return true
    return startswith(norm_path, norm_root * Base.Filesystem.path_separator)
end


function serve_static(req)
    path = HTTP.URI(req.target).path
    # Default to node edition
    relative_path = path == "/" ? "index-node.html" : lstrip(path, '/')
    root = static_dir()
    filepath = normpath(joinpath(root, relative_path))

    if !_is_subpath(filepath, root) || !isfile(filepath)
        return HTTP.Response(404, "Not found")
    end

    ext = splitext(filepath)[2]
    mime = Dict(
        ".html" => "text/html",
        ".js" => "application/javascript",
        ".css" => "text/css",
        ".json" => "application/json",
        ".png" => "image/png",
        ".svg" => "image/svg+xml",
    )
    content_type = get(mime, ext, "application/octet-stream")
    return HTTP.Response(200, ["Content-Type" => content_type], read(filepath))
end

# ─── Router ───
const API_ROUTES = Dict{String, Function}(
    "/api/build_atlas" => handle_build_atlas,
    "/api/query_atlas" => handle_query_atlas,
    "/api/build_atlas_library" => handle_build_atlas_library,
    "/api/merge_atlas_library" => handle_merge_atlas_library,
    "/api/run_inverse_design" => handle_run_inverse_design,
    "/api/build_model" => handle_build_model,
    "/api/find_vertices" => handle_find_vertices,
    "/api/build_graph" => handle_build_graph,
    "/api/siso_paths" => handle_siso_paths,
    "/api/siso_polyhedra" => handle_siso_polyhedra,
    "/api/siso_path_condition" => handle_siso_path_condition,
    "/api/siso_trajectory" => handle_siso_trajectory,
    "/api/behavior_families" => handle_behavior_families,
    "/api/rop_cloud" => handle_rop_cloud,
    "/api/vertex_detail" => handle_vertex_detail,
    "/api/fret_heatmap" => handle_fret_heatmap,
    "/api/parameter_scan_1d" => handle_parameter_scan_1d,
    "/api/parameter_scan_2d" => handle_parameter_scan_2d,
    "/api/atlas_landscape_2d" => handle_atlas_landscape_2d,
    "/api/rop_polyhedron" => handle_rop_polyhedron,
    "/api/debug_logs" => handle_debug_logs,
)

function router(req)
    # CORS preflight
    if req.method == "OPTIONS"
        return HTTP.Response(200, [
            "Access-Control-Allow-Origin" => "*",
            "Access-Control-Allow-Methods" => "POST, GET, OPTIONS",
            "Access-Control-Allow-Headers" => "Content-Type, X-Biocircuits-Explorer-Debug-Client, X-ROP-Debug-Client",
        ])
    end

    path = HTTP.URI(req.target).path

    if haskey(API_ROUTES, path) && req.method != "POST"
        return error_response("Method not allowed"; status=405)
    end

    if haskey(API_ROUTES, path)
        client_id = debug_client_id_from_request(req)
        return with_debug_client_scope(client_id) do
            try
                return API_ROUTES[path](req)
            catch e
                @error "API error" path exception=(e, catch_backtrace())
                # Distinguish between user errors (400) and server errors (500)
                if is_request_error(e)
                    return error_response("Invalid request: $(sprint(showerror, e))"; status=400)
                else
                    return error_response("Internal server error"; status=500)
                end
            end
        end
    end

    # Static files
    return serve_static(req)
end

# ─── Start server ───
function main()
    install_debug_logger!()
    port = resolve_port()
    expected_parent_pid = configured_parent_pid()
    @info "ROP Web Server starting on http://localhost:$port"
    @info "Static files from: $(static_dir())"
    @info "Session TTL: $(SESSION_TTL)s, cleanup interval: $(SESSION_CLEANUP_INTERVAL)s"

    # Start session cleanup task in background
    @async cleanup_old_sessions()
    if expected_parent_pid !== nothing
        @async parent_watchdog_loop(expected_parent_pid)
    end

    HTTP.serve(router, "0.0.0.0", port)
end

function julia_main()::Cint
    try
        main()
        return 0
    catch err
        append_debug_log("ERROR", "Backend crashed during startup"; module_name=:BiocircuitsExplorerBackend, details=sprint(showerror, err, catch_backtrace()))
        showerror(stderr, err, catch_backtrace())
        println(stderr)
        return 1
    end
end

end # module
