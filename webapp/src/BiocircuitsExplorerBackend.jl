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
export NetworkIR, DesignSpec, SpeciesDecl, ReactionDecl, ObservableDecl, ParameterDistribution, Provenance, IRValidationError
export NETWORK_IR_SCHEMA_VERSION, DESIGN_SPEC_SCHEMA_VERSION
export parse_network_ir, parse_design_spec, network_ir_to_dict, design_spec_to_dict
export network_ir_from_legacy, network_ir_to_legacy_inputs, design_spec_to_legacy_request
export network_ir_hash, design_spec_hash, is_network_ir, is_legacy_network_payload
export network_ir_to_sbml, sbml_to_network_ir
export enumerate_network_specs
export build_behavior_atlas, build_behavior_atlas_from_spec, looks_like_atlas_corpus
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
export submit_biocircuits_job_from_spec, get_biocircuits_job, get_biocircuits_job_result, cancel_biocircuits_job
export run_biocircuits_job_payload, run_biocircuits_job_from_uri
export biocircuits_explorer_version, biocircuits_explorer_build_info
export RESULT_ARTIFACT_SCHEMA_VERSION, artifact_metadata, attach_artifact!, wrap_artifact

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
import EzXML

include(joinpath(@__DIR__, "config.jl"))
include(joinpath(@__DIR__, "session_store.jl"))
include(joinpath(@__DIR__, "model_cache.jl"))
include(joinpath(@__DIR__, "debug_log.jl"))
include(joinpath(@__DIR__, "observability.jl"))
include(joinpath(@__DIR__, "serialization.jl"))
include(joinpath(@__DIR__, "reaction_parser.jl"))
include(joinpath(@__DIR__, "static_assets.jl"))
using .SessionStore: get_session, set_session
using .DebugLog: append_debug_log, with_debug_client_scope,
                  debug_client_id_from_request, install_debug_logger!
using .Observability: counter_inc!, gauge_set!, hist_observe!,
                       render_prometheus, log_request_json,
                       json_logs_enabled, iso_timestamp
using .Serialization: mat2vv, json_safe_value, json_safe_real, json_safe_profile,
                       json_response, error_response, read_json, is_request_error
using .ReactionParser: parse_term, parse_side, parse_reactions, parse_network_structure,
                       build_model, default_log_qK, fixed_qK_or_default
using .StaticAssets: static_dir, serve_static

# Shared canonicalization / content-identity primitives (raw-JSON access, the
# canonical-JSON hasher, and the graph-canonical network code). Included here —
# before atlas.jl/inverse_design.jl/ir.jl — so the IR/result substrate no longer
# depends backwards on those big files just to hash an artifact.
include(joinpath(@__DIR__, "canonicalization.jl"))

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

configured_parent_pid() = parse_optional_int(Config.parent_pid_raw())

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

# Internal helper: like `fixed_qK_or_default`, but uses the raw-JSON accessors
# defined in atlas.jl so it can read JSON3 objects without a `haskey` method.
function _fixed_qK_or_default_raw(body, model, kd::AbstractVector{<:Real})
    fixed_qK = if _raw_haskey(body, :fixed_qK)
        Float64.(collect(_raw_get(body, :fixed_qK, Float64[])))
    else
        default_log_qK(model, kd)
    end
    length(fixed_qK) == model.n ||
        error("Length of `fixed_qK` must equal the full q/K dimension ($(model.n)).")
    return fixed_qK
end

function handle_debug_logs(req)
    body = read_json(req)
    result = DebugLog.read_logs(
        after_seq = Int(get(body, :after_seq, 0)),
        limit     = Int(get(body, :limit, 300)),
        client_id = debug_client_id_from_request(req),
    )
    return json_response(Dict(
        "entries"  => result.entries,
        "next_seq" => result.next_seq,
        "total"    => result.total,
        "limit"    => result.limit,
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
include(joinpath(@__DIR__, "inverse_design.jl"))
include(joinpath(@__DIR__, "ir.jl"))
include(joinpath(@__DIR__, "sbml.jl"))
include(joinpath(@__DIR__, "version.jl"))
include(joinpath(@__DIR__, "result_artifact.jl"))
include(joinpath(@__DIR__, "auth.jl"))
include(joinpath(@__DIR__, "jobs.jl"))

export verify_cognito_jwt

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

# Label schemas for /metrics. Lives here (not in Observability) so that
# adding a new metric to handle_metrics doesn't require touching the
# storage layer. Tuple order must match the tuples passed to counter_inc!
# / hist_observe! / gauge_set! at the call sites.
const _METRIC_LABEL_SCHEMAS = Dict{String, Tuple}(
    "bcx_http_requests_total"         => (:method, :path, :status),
    "bcx_http_request_duration_seconds" => (:method, :path),
    "bcx_uptime_seconds"              => (),
    "bcx_sessions_active"             => (),
    "bcx_build_info"                  => (:version, :revision),
)

# GET /metrics — Prometheus scrape endpoint. Returns text exposition v0.0.4.
# Caution: in production this should be exposed only on an internal network
# (or behind auth) because path labels could leak API shape. The nginx
# config defaults to proxying it through, so deployments that don't want
# /metrics public must block it at the edge.
function handle_metrics(req)
    # Refresh dynamic gauges on each scrape. These are cheap (an integer
    # session count, a subtraction for uptime, a string lookup for build
    # info) so running them inline is fine.
    Observability.gauge_set!("bcx_uptime_seconds", (),
        (time_ns() - _STARTUP_TIME_NS[]) / 1e9)
    Observability.gauge_set!("bcx_sessions_active", (),
        SessionStore.session_count())
    Observability.gauge_set!("bcx_build_info",
        (biocircuits_explorer_version(),
         strip(get(ENV, "BIOCIRCUITS_EXPLORER_REVISION", "unknown"))),
        1.0)

    text = Observability.render_prometheus(_METRIC_LABEL_SCHEMAS)
    return HTTP.Response(200,
        ["Content-Type" => "text/plain; version=0.0.4; charset=utf-8"],
        text)
end

# GET /health — liveness probe. Returns 200 as long as the Julia process is
# answering. Cheap, no I/O. Used by container orchestrators (Docker
# HEALTHCHECK, Kubernetes livenessProbe, AWS ALB target groups) to decide
# whether to restart the instance.
function handle_health(req)
    initialized = _STARTUP_TIME_NS[] != 0
    uptime_s = initialized ? (time_ns() - _STARTUP_TIME_NS[]) / 1e9 : 0.0
    return json_response(Dict{String, Any}(
        "status"          => "ok",
        "version"         => biocircuits_explorer_version(),
        "revision"        => strip(get(ENV, "BIOCIRCUITS_EXPLORER_REVISION", "unknown")),
        "uptime_seconds"  => round(uptime_s; digits=3),
    ))
end

# GET /ready — readiness probe. Reports whether the app can serve real
# traffic. Fails closed (503) if any check is missing so a load balancer can
# route around the instance until it recovers. Distinct from /health: a
# crash-looping container is unhealthy; a still-warming container is
# unready but should not be restarted.
function handle_ready(req)
    initialized = _STARTUP_TIME_NS[] != 0
    static_ok = isdir(static_dir())
    checks = Dict{String, Any}(
        "module_initialized" => initialized,
        "static_assets"      => static_ok,
    )
    ready = initialized && static_ok
    return json_response(Dict{String, Any}(
        "status" => ready ? "ready" : "not_ready",
        "checks" => checks,
    ); status = ready ? 200 : 503)
end

function handle_version(req)
    info = Dict{String, Any}(biocircuits_explorer_build_info())
    # API protocol identity, separate from the application build metadata above.
    # `api_version` is the canonical current surface; `api_supported` lets a
    # client decide whether to fall back; `api_legacy_sunset` is the ISO date
    # after which we stop serving the bare /api/<endpoint> alias.
    info["api_version"] = API_CURRENT_VERSION
    info["api_supported"] = [API_CURRENT_VERSION]
    info["api_legacy_sunset"] = API_LEGACY_SUNSET
    return json_response(info)
end

# Public auth bootstrap. Frontend calls this once on load to discover whether
# Cognito is configured for this deployment, and (if so) which user pool /
# client / hosted UI domain to redirect users to. Returns "enabled: false"
# in dev mode (no Cognito), so the SPA can degrade gracefully to local-only.
function handle_auth_config(req)
    pool_id = Config.cognito_user_pool_id()
    if isempty(pool_id)
        return json_response(Dict{String, Any}("enabled" => false))
    end
    return json_response(Dict{String, Any}(
        "enabled" => true,
        "cognito_region" => Config.cognito_region(),
        "cognito_user_pool_id" => pool_id,
        "cognito_app_client_id" => Config.cognito_app_client_id(),
        "cognito_domain" => Config.cognito_domain(),
        "scopes" => ["openid", "email", "profile"],
        "response_type" => "code",
    ))
end

# POST /api/v1/export/sbml — accepts a NetworkIR (top-level or under
# `network`) or the legacy {reactions, kd} shape, returns an SBML L3 string.
function handle_export_sbml(req)
    body = read_json(req)
    network_payload = _raw_haskey(body, :network) ? _raw_get(body, :network, nothing) : body
    network = try
        parse_network_ir(network_payload)
    catch err
        err isa IRValidationError &&
            return error_response(sprint(showerror, err); status = 400)
        rethrow(err)
    end
    return json_response(Dict(
        "sbml" => network_ir_to_sbml(network),
        "label" => network.label,
    ))
end

# POST /api/v1/import/sbml — accepts {sbml: "<xml string>"}, returns the
# parsed NetworkIR plus a list of warnings about anything not representable.
function handle_import_sbml(req)
    body = read_json(req)
    xml = _raw_get(body, :sbml, nothing)
    (xml isa AbstractString && !isempty(strip(xml))) ||
        return error_response("Missing required field `sbml` (the SBML document as a string)"; status = 400)
    network, warnings = sbml_to_network_ir(String(xml))
    return json_response(Dict(
        "network_ir" => network_ir_to_dict(network),
        "warnings" => warnings,
    ))
end

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

function handle_build_model(req)
    body = read_json(req)
    sid = get(body, :session_id, string(rand(UInt32), base=16))

    # Accept either a NetworkIR (top-level or under `network`) or the legacy
    # `{reactions, kd}` shape — parse_network_ir bridges both.
    network_payload = _raw_haskey(body, :network) ? _raw_get(body, :network, nothing) : body
    network = try
        parse_network_ir(network_payload)
    catch err
        err isa IRValidationError &&
            return error_response(sprint(showerror, err); status = 400)
        rethrow(err)
    end

    # The compiled model is a pure derived cache of the NetworkIR, keyed by hash;
    # identical IRs share a bundle. The session is just a convenience handle
    # pointing at the same bundle object.
    bundle = try
        build_model_bundle(network)
    catch err
        err isa ArgumentError && return error_response(sprint(showerror, err); status = 400)
        rethrow(err)
    end
    set_session(String(sid), bundle)

    model = bundle["model"]
    return json_response(Dict(
        "session_id" => sid,
        "n" => model.n, "d" => model.d, "r" => model.r,
        "species" => string.(bundle["species"]),
        "free_species" => string.(bundle["free_syms"]),
        "product_species" => string.(bundle["prod_syms"]),
        "x_sym" => string.(model.x_sym),
        "q_sym" => string.(model.q_sym),
        "K_sym" => string.(model.K_sym),
        "kd" => collect(bundle["kd"]),
        "N" => mat2vv(Matrix(model.N)),
        "L" => mat2vv(Matrix(model.L)),
        "network_ir" => bundle["network_ir"],
        "network_ir_hash" => bundle["network_ir_hash"],
        # Self-describing provenance (non-breaking sibling): which IR this model
        # was compiled from, by which algorithm/version.
        "artifact" => artifact_metadata("build_model";
            input_hashes = Dict("network_ir_hash" => bundle["network_ir_hash"])),
    ))
end

function handle_ir_network_validate(req)
    body = read_json(req)
    payload = _raw_haskey(body, :network) ? _raw_get(body, :network, nothing) : body
    try
        net = parse_network_ir(payload)
        return json_response(Dict{String, Any}(
            "valid" => true,
            "ir_schema_version" => net.ir_schema_version,
            "network" => network_ir_to_dict(net),
            "hash" => network_ir_hash(net),
        ))
    catch err
        err isa IRValidationError ||
            return error_response("Invalid network payload: $(sprint(showerror, err))"; status=400)
        return json_response(Dict{String, Any}(
            "valid" => false,
            "section" => "network",
            "error" => err.msg,
            "path" => err.path,
        ); status=400)
    end
end

function handle_ir_design_validate(req)
    body = read_json(req)
    design_payload = _raw_haskey(body, :design) ? _raw_get(body, :design, nothing) : body
    network_payload = _raw_haskey(body, :network) ? _raw_get(body, :network, nothing) : nothing

    network = nothing
    if network_payload !== nothing
        try
            network = parse_network_ir(network_payload)
        catch err
            err isa IRValidationError ||
                return error_response("Invalid network payload: $(sprint(showerror, err))"; status=400)
            return json_response(Dict{String, Any}(
                "valid" => false,
                "section" => "network",
                "error" => err.msg,
                "path" => err.path,
            ); status=400)
        end
    end

    try
        ds = parse_design_spec(design_payload)
        out = Dict{String, Any}(
            "valid" => true,
            "ir_schema_version" => ds.ir_schema_version,
            "design" => design_spec_to_dict(ds),
            "hash" => design_spec_hash(ds),
        )
        if network !== nothing
            out["legacy_request"] = design_spec_to_legacy_request(ds; network=network)
            out["network_hash"] = network_ir_hash(network)
        end
        return json_response(out)
    catch err
        err isa IRValidationError ||
            return error_response("Invalid design payload: $(sprint(showerror, err))"; status=400)
        return json_response(Dict{String, Any}(
            "valid" => false,
            "section" => "design",
            "error" => err.msg,
            "path" => err.path,
        ); status=400)
    end
end

function handle_find_vertices(req)
    body = read_json(req)
    bundle, err = _resolve_bundle_or_response(body)
    err === nothing || return err

    model = bundle["model"]

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
    bundle, err = _resolve_bundle_or_response(body)
    err === nothing || return err

    model = bundle["model"]
    graph_mode = Symbol(String(get(body, :graph_mode, "qk")))
    change_qK = haskey(body, :change_qK) ? Symbol(String(body[:change_qK])) : nothing

    data = graph_to_dict(model; graph_mode=graph_mode, change_qK=change_qK)
    return json_response(data)
end

function handle_siso_paths(req)
    body = read_json(req)
    bundle, err = _resolve_bundle_or_response(body)
    err === nothing || return err

    model = bundle["model"]
    change_qK = Symbol(body[:change_qK])

    siso = SISOPaths(model, change_qK)
    bundle["siso_$(body[:change_qK])"] = siso

    data = siso_to_dict(model, siso)
    return json_response(data)
end

function handle_siso_polyhedra(req)
    body = read_json(req)
    bundle, err = _resolve_bundle_or_response(body)
    err === nothing || return err

    model = bundle["model"]
    change_key = "siso_$(body[:change_qK])"
    siso = get(bundle, change_key, nothing)
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
    bundle, err = _resolve_bundle_or_response(body)
    err === nothing || return err

    model = bundle["model"]
    change_key = "siso_$(body[:change_qK])"
    siso = get(bundle, change_key, nothing)
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
    bundle, err = _resolve_bundle_or_response(body)
    err === nothing || return err

    model = bundle["model"]
    change_key = "siso_$(body[:change_qK])"
    siso = get(bundle, change_key, nothing)
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
    bundle, err = _resolve_bundle_or_response(body)
    err === nothing || return err

    model = bundle["model"]
    change_qK = Symbol(body[:change_qK])
    observe_x = haskey(body, :observe_x) ? Symbol(body[:observe_x]) : error("observe_x is required")
    path_scope = Symbol(get(body, :path_scope, "feasible"))
    min_volume_mean = Float64(get(body, :min_volume_mean, 0.0))
    deduplicate = Bool(get(body, :deduplicate, true))
    keep_singular = Bool(get(body, :keep_singular, true))
    keep_nonasymptotic = Bool(get(body, :keep_nonasymptotic, false))
    compute_volume = Bool(get(body, :compute_volume, true))

    change_key = "siso_$(body[:change_qK])"
    siso = get(bundle, change_key, nothing)
    if siso === nothing
        siso = SISOPaths(model, change_qK)
        bundle[change_key] = siso
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
            bundle, err = _resolve_bundle_or_response(body)
            err === nothing || return err
            String.(bundle["rules"])
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
        bundle, err = _resolve_bundle_or_response(body)
        err === nothing || return err

        model = bundle["model"]
        kd = bundle["kd"]
        prod_syms = Symbol.(bundle["prod_syms"])

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
    bundle, err = _resolve_bundle_or_response(body)
    err === nothing || return err

    model = bundle["model"]
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
    bundle, err = _resolve_bundle_or_response(body)
    err === nothing || return err

    model = bundle["model"]
    kd = bundle["kd"]
    prod_syms = Symbol.(bundle["prod_syms"])

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
    bundle, err = _resolve_bundle_or_response(body)
    err === nothing || return err

    model = bundle["model"]

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

    # Fixed parameters (full log qK). If omitted, keep imported/session Kd values.
    fixed_qK = try
        fixed_qK_or_default(body, model, Float64.(bundle["kd"]))
    catch e
        return error_response(sprint(showerror, e); status=400)
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
        "fixed_qK" => collect(fixed_qK),
    ))
end

function handle_parameter_scan_2d(req)
    body = read_json(req)
    bundle, err = _resolve_bundle_or_response(body)
    err === nothing || return err

    model = bundle["model"]

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

    # Fixed parameters (full log qK). If omitted, keep imported/session Kd values.
    fixed_qK = try
        fixed_qK_or_default(body, model, Float64.(bundle["kd"]))
    catch e
        return error_response(sprint(showerror, e); status=400)
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
        "fixed_qK" => collect(fixed_qK),
    ))
end

function _atlas_landscape_model_from_spec(body)
    # Prefer a resolvable model bundle (network / hash / session); fall back to a
    # raw {reactions, kd} build for fully stateless landscape calls.
    if _raw_haskey(body, :network) || _raw_haskey(body, :network_ir_hash) || _raw_haskey(body, :session_id)
        bundle = resolve_model_bundle(body)
        return (
            model = bundle["model"],
            rules = String.(bundle["rules"]),
            kd = Float64.(bundle["kd"]),
        )
    end

    _raw_haskey(body, :reactions) || error("Landscape scan requires `network`, `session_id`, or `reactions`.")
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

    fixed_qK = _fixed_qK_or_default_raw(body, model, ctx.kd)

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
    bundle, err = _resolve_bundle_or_response(body)
    err === nothing || return err

    model = bundle["model"]

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

handle_local_image(req) =
    StaticAssets.handle_local_image(req; has_parent_pid = configured_parent_pid() !== nothing)

include(joinpath(@__DIR__, "routing.jl"))

end # module
