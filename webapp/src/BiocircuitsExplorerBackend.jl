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
export DESIGNABILITY_SPEC_VERSION, normalize_designability_spec, design_screen_from_spec

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

# BindingAndCatalysis keeps this accessor internal, while the web backend uses
# it throughout SISO graph and trajectory serialization.
get_change_qK_idx(args...) = BindingAndCatalysis.get_change_qK_idx(args...)

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
            edge_labels = Dict(Edge(src(e), dst(e)) => "+" * string(qK_sym(model)[get_change_qK_idx(siso)]) for e in Graphs.edges(siso_graph))
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
    change_sym = string(qK_sym(model)[get_change_qK_idx(siso)])
    paths_data = []
    for (i, path) in enumerate(siso.rgm_paths)
        perms = [collect(get_perm(model, idx)) for idx in path]
        push!(paths_data, Dict(
            "idx" => i,
            "vertex_indices" => collect(path),
            "perms" => perms,
        ))
    end

    sources_perms = [collect(get_perm(model, s)) for s in get_sources(siso)]
    sinks_perms = [collect(get_perm(model, s)) for s in get_sinks(siso)]

    return Dict(
        "change_qK" => change_sym,
        "change_qK_idx" => get_change_qK_idx(siso),
        "sources" => collect(get_sources(siso)),
        "sinks" => collect(get_sinks(siso)),
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

    change_idx = get_change_qK_idx(siso)

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
        "sources" => collect(get_sources(siso)),
        "sinks" => collect(get_sinks(siso)),
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
# The Latent-Atlas SISO phenotyper — the SAME labeller that built the dose atlas. Exposed so the
# design agent verifies a candidate's dose-response shape CONSISTENTLY with the atlas labels
# (shape_support over the Kd prior Π), not via the different ROP-family view of behavior_families.
include(joinpath(@__DIR__, "latent_atlas", "phenotype_pipeline.jl"))
using .PhenotypePipeline: phenotype_profile, PhenotyperPolicy, ParameterPrior, LogUniform, PointMass

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
        "change_qK" => string(qK_sym(model)[get_change_qK_idx(siso)]),
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
        "change_qK" => string(qK_sym(model)[get_change_qK_idx(siso)]),
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

# Classify a network's 1-input dose-response with the Latent-Atlas SISO phenotyper — the SAME
# labeller (shape_support over the Kd prior Π) that built the dose dataset, so the agent's
# verification matches the atlas. Distinct from /api/behavior_families (ROP path families).
function handle_phenotype_classify(req)
    body = read_json(req)
    bundle, err = _resolve_bundle_or_response(body)
    err === nothing || return err
    model = bundle["model"]
    haskey(body, :input_symbol) || return error_response("input_symbol is required"; status=400)
    haskey(body, :output_expr) || return error_response("output_expr is required"; status=400)
    input_sym = Symbol(String(body[:input_symbol]))
    output_expr = String(body[:output_expr])
    K = clamp(Int(get(body, :K, 8)), 1, 64)
    # Canonical prior Π: Kd ~ LogUniform(-3,3), totals pinned — matches the dose dataset default.
    prior = ParameterPrior(; default_kd = LogUniform(-3.0, 3.0), default_total = PointMass(0.0))
    policy = PhenotyperPolicy(; K = K)
    prof = try
        phenotype_profile(model; input_sym = input_sym, output_expr = output_expr, prior = prior, policy = policy)
    catch e
        return error_response("phenotype_classify failed: $(sprint(showerror, e))"; status=400)
    end
    dom = prof.dominant_shape
    support = get(prof.shape_fractions, dom == :none ? :flat : dom, 0.0)
    return json_response(Dict(
        "dominant_shape" => String(dom),
        "shape_support" => support,
        "shape_fractions" => Dict(String(k) => v for (k, v) in prof.shape_fractions),
        # the actual ±-pattern behind the verdict (e.g. [1,-1,1] = rise-fall-rise),
        # so the agent/UI can show RO sign oscillation, not just the class name
        "sign_seq" => prof.sign_seq, "n_sign_changes" => prof.n_sign_changes,
        "min_swing_log10" => prof.min_swing_log10,
        "n_draws" => prof.n_draws, "n_evaluated" => prof.n_evaluated, "n_failed" => prof.n_failed,
        "input_symbol" => String(input_sym), "output" => prof.output,
        "phenotyper_version" => prof.phenotyper_version,
    ))
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

# ─── ParameterPlacer: constructive (q,K) placement for a target reaction order ───
#
# Inlined from the verified prototype `webapp/scripts/synth/parameter_placer.jl`
# (its `place_for_target_slope(...; kd_bounds=...)` path). The prototype is NOT
# `include`d here on purpose: its top-level `Pkg.activate(Bnc_julia)` would switch
# the running backend's active project. Every engine call below
# (find_all_vertices!, SISOPaths, get_behavior_families, get_polyhedron,
# get_one_inner_point, get_C_C0_nullity_qK, qK2x, q_sym, show_dominant_condition,
# scan_parameter_1d, parse_linear_combination) is already in scope via
# `using BindingAndCatalysis`; `build_model` via `using .ReactionParser`;
# `Polyhedra` via `using Polyhedra`. Pure polytope geometry in the solve;
# sampling is used only for the forward verification and the dose-response curve.

# Split an engine log10-(q,K) vector into linear-space kd (Kd1..Kdr) + totals dict.
function _placer_split_log_qK(model, logqK::AbstractVector{<:Real})
    d = model.d; r = model.r
    @assert length(logqK) == d + r
    kd = exp10.(Float64.(logqK[d+1:d+r]))
    qsyms = Symbol.(string.(q_sym(model)))
    totals = Dict{Symbol, Float64}()
    for i in 1:d
        totals[qsyms[i]] = exp10(Float64(logqK[i]))
    end
    return kd, totals
end

# Forward finite-difference local reaction order at a concrete operating point.
function _placer_measure_local_RO(model, logqK, input_idx::Int, output_idx::Int; h::Real = 0.02)
    base = collect(Float64.(logqK))
    f(delta) = begin
        p = copy(base); p[input_idx] += delta
        qK2x(model, p; input_logspace = true, output_logspace = true)[output_idx]
    end
    return (f(+h) - f(-h)) / (2h)
end

# Box rows encoding lo <= log10(Kd_i) <= hi for every binding constant, in the
# get_polyhedron(C, C0) sign convention (rows mean -C . x <= C0).
function _placer_kd_box_rows(model, lo::Real, hi::Real)
    d = model.d; r = model.r; n = model.n
    rows = Vector{Vector{Float64}}(); rhsv = Float64[]
    for i in (d+1):(d+r)
        e = zeros(Float64, n); e[i] = 1.0
        push!(rows, -e); push!(rhsv, Float64(hi))    # z_i <= hi
        push!(rows,  e); push!(rhsv, -Float64(lo))   # z_i >= lo
    end
    C = reduce(vcat, (reshape(c, 1, n) for c in rows))
    return C, rhsv
end

# Most-interior point of a (bounded-or-coned) polyhedron: Chebyshev center when
# finite, else a moderate inner point.
function _placer_most_interior_point(poly; extend::Real = 3)
    try
        ctr, rad = Polyhedra.hchebyshevcenter(poly; verbose = 0, proper = false)
        if isfinite(rad)
            return collect(Float64.(ctr)), Float64(rad), :chebyshev
        end
    catch
        # LP backend (CDD) cannot solve this center; fall through.
    end
    ip = collect(Float64.(get_one_inner_point(poly; rand_line = false, rand_ray = false, extend = extend)))
    return ip, NaN, :inner_point
end

# Constructive, physical-Kd-bounded placement of a (q,K) point realizing
# `target_RO` (local log-log slope of `output_sym` w.r.t. the total of
# `input_sym`). Returns a NamedTuple; on infeasible (target regime ∩ Kd-box
# empty, or no regime matches) returns feasible=false with closest_RO/vertex.
function placer_place_bounded(rules::Vector{String}, input_sym, output_sym,
                              target_RO::Real; tol::Real = 0.05, verify_h::Real = 0.02,
                              kd_bounds::Tuple{<:Real, <:Real})
    lo, hi = Float64(kd_bounds[1]), Float64(kd_bounds[2])
    lo < hi || error("kd_bounds must be (lo_log10, hi_log10) with lo < hi")
    input_sym = Symbol(input_sym); output_sym = Symbol(output_sym)

    model, species, _, _ = build_model(rules, fill(1.0, length(rules)))
    find_all_vertices!(model)
    input_idx = locate_sym_qK(model, input_sym)
    input_idx === nothing && error("Unknown input parameter: $input_sym")
    output_idx = locate_sym_x(model, output_sym)
    output_idx === nothing && error("Unknown output species: $output_sym")

    siso = SISOPaths(model, input_sym)
    bf = get_behavior_families(siso; observe_x = output_sym, path_scope = :feasible,
        deduplicate = false, keep_singular = true, keep_nonasymptotic = true,
        compute_volume = false)

    chosen_v = nothing; chosen_ro = nothing
    best_miss = Inf; best_v = nothing; best_ro = nothing
    for pr in bf.path_records
        pr.included || continue
        vtxs = pr.vertex_indices; ros = pr.exact_profile
        length(vtxs) == length(ros) || continue
        for (v, ro) in zip(vtxs, ros)
            isfinite(ro) || continue
            miss = abs(ro - target_RO)
            if miss < best_miss; best_miss = miss; best_v = v; best_ro = ro; end
            if miss <= tol; chosen_v = v; chosen_ro = ro; break; end
        end
        chosen_v === nothing || break
    end

    if chosen_v === nothing
        return (; feasible = false, kd_bounds = (lo, hi),
            reason = "No regime realizes target_RO=$target_RO (tol=$tol) for output " *
                     "$output_sym vs input $input_sym. Closest achievable RO is " *
                     "$best_ro at vertex $best_v. This target is infeasible for this topology.",
            closest_RO = best_ro, closest_vertex = best_v,
            input_idx, output_idx, model)
    end

    vC, vC0, vnull = get_C_C0_nullity_qK(model, chosen_v)
    vC = Matrix{Float64}(vC); vC0 = Vector{Float64}(vC0)
    if vnull > 0
        eqC = vC[1:vnull, :];     eqC0 = vC0[1:vnull]
        inC = vC[vnull+1:end, :]; inC0 = vC0[vnull+1:end]
    else
        eqC = zeros(0, model.n);  eqC0 = Float64[]
        inC = vC;                 inC0 = vC0
    end
    boxC, boxC0 = _placer_kd_box_rows(model, lo, hi)
    poly = get_polyhedron(vcat(eqC, inC, boxC), vcat(eqC0, inC0, boxC0), size(eqC, 1))

    if isempty(poly)
        return (; feasible = false, kd_bounds = (lo, hi),
            reason = "Target regime ∩ Kd-box [1e$lo, 1e$hi] is empty. " *
                     "Closest feasible RO under these bounds is $best_ro at vertex $best_v.",
            closest_RO = best_ro, closest_vertex = best_v,
            input_idx, output_idx, model)
    end

    logqK, cheb_r, mode = _placer_most_interior_point(poly; extend = 3)
    kd, totals = _placer_split_log_qK(model, logqK)
    dominance = string.(show_dominant_condition(model, chosen_v))
    measured = _placer_measure_local_RO(model, logqK, input_idx, output_idx; h = verify_h)
    pass = abs(measured - target_RO) <= max(tol, 5 * verify_h)

    return (; feasible = true, kd, totals, vertex_idx = chosen_v, predicted_RO = chosen_ro,
            dominance_ordering = dominance, log_qK = logqK, measured_RO = measured, pass,
            kd_bounds = (lo, hi), chebyshev_radius = cheb_r, solve_mode = mode,
            input_idx, output_idx, model, species)
end

# Verifying dose-response curve at the solved (kd, totals): build the model with
# the solved kd, set the solved totals as background, sweep the input total, and
# observe the output — reusing the exact `scan_parameter_1d` engine path of
# `handle_parameter_scan_1d` so the result has the `plotParameterScan1D` shape.
function placer_dose_response(rules::Vector{String}, kd::Vector{Float64},
                              totals::Dict{Symbol, Float64}, input_sym::Symbol,
                              output_expr::String; param_min = -6.0, param_max = 6.0,
                              n_points = 200)
    model, _, _, _ = build_model(rules, kd)
    input_idx = locate_sym_qK(model, input_sym)
    coeffs = parse_linear_combination(model, output_expr)
    qsyms = Symbol.(string.(q_sym(model)))
    fixed_qK = zeros(Float64, model.n)
    for i in 1:model.d
        fixed_qK[i] = log10(get(totals, qsyms[i], 1.0))
    end
    for j in 1:model.r
        fixed_qK[model.d + j] = log10(kd[j])
    end
    fixed_params = deleteat!(copy(fixed_qK), input_idx)
    n_pts = clamp(Int(n_points), 10, 1000)
    param_range = collect(range(Float64(param_min), Float64(param_max), length = n_pts))
    param_vals, output_traj, regimes = scan_parameter_1d(
        model, input_idx, param_range, [coeffs], fixed_params;
        input_logspace = true, output_logspace = true)
    return Dict(
        "param_symbol" => string(input_sym),
        "param_values" => param_vals,
        "output_exprs" => [output_expr],
        "output_traj"  => mat2vv(output_traj),
        "regimes"      => regimes,
        "x_sym"        => string.(model.x_sym),
    )
end

# POST /api/place_parameters
# Body: { rules:[...] (or session_id/network/hash), input_sym, output_sym,
#         target_ro, kd_bounds:[lo,hi]|null, tol|null }
function handle_place_parameters(req)
    body = read_json(req)

    # Reactions: explicit `rules` win; otherwise resolve a model bundle and use
    # its rules (session_id / network / network_ir_hash).
    rules = if _raw_haskey(body, :rules)
        String.(collect(_raw_get(body, :rules, String[])))
    else
        bundle, err = _resolve_bundle_or_response(body)
        err === nothing || return err
        String.(collect(bundle["rules"]))
    end
    isempty(rules) && return error_response("No reactions: provide `rules` or a resolvable model"; status = 400)

    _raw_haskey(body, :input_sym)  || return error_response("`input_sym` is required"; status = 400)
    _raw_haskey(body, :output_sym) || return error_response("`output_sym` is required"; status = 400)
    input_sym  = Symbol(_raw_get(body, :input_sym, ""))
    output_sym = Symbol(_raw_get(body, :output_sym, ""))
    target_ro  = Float64(_raw_get(body, :target_ro, 1.0))
    tol        = Float64(_raw_get(body, :tol, 0.05))

    kb_raw = _raw_get(body, :kd_bounds, nothing)
    kd_bounds = if kb_raw === nothing
        (-3.0, 3.0)
    else
        kbv = Float64.(collect(kb_raw))
        length(kbv) == 2 || return error_response("`kd_bounds` must be [lo_log10, hi_log10]"; status = 400)
        (kbv[1], kbv[2])
    end

    res = try
        placer_place_bounded(rules, input_sym, output_sym, target_ro; tol = tol, kd_bounds = kd_bounds)
    catch e
        return error_response("ParameterPlacer failed: $(sprint(showerror, e))"; status = 400)
    end

    if !res.feasible
        return json_response(Dict(
            "error" => res.reason,
            "closest_RO" => json_safe_value(get(res, :closest_RO, nothing)),
            "closest_vertex" => get(res, :closest_vertex, nothing),
        ))
    end

    kd_vec = collect(Float64.(res.kd))
    curve = try
        placer_dose_response(rules, kd_vec, res.totals, input_sym, String(output_sym))
    catch e
        return error_response("Dose-response curve failed: $(sprint(showerror, e))"; status = 400)
    end

    return json_response(Dict(
        "kd"                  => kd_vec,
        "totals"              => Dict(string(k) => v for (k, v) in res.totals),
        "vertex_idx"          => res.vertex_idx,
        "predicted_RO"        => json_safe_real(res.predicted_RO),
        "measured_RO"         => json_safe_real(res.measured_RO),
        "pass"                => res.pass,
        "dominance_ordering"  => res.dominance_ordering,
        "kd_bounds"           => collect(res.kd_bounds),
        "solve_mode"          => string(res.solve_mode),
        "dose_response_curve" => curve,
    ))
end

# Achievable reaction-order MENU for a network: the distinct finite RO values the
# output can take across the feasible dominance regimes (the quantized ladder),
# each with a representative regime + its dominance ordering. Enumerated from the
# regimes (no sampling) — mirrors placer_place_bounded's path-record loop.
function placer_menu(rules::Vector{String}, input_sym, output_sym)
    input_sym = Symbol(input_sym); output_sym = Symbol(output_sym)
    model, _, _, _ = build_model(rules, fill(1.0, length(rules)))
    find_all_vertices!(model)
    siso = SISOPaths(model, input_sym)
    bf = get_behavior_families(siso; observe_x = output_sym, path_scope = :feasible,
        deduplicate = false, keep_singular = true, keep_nonasymptotic = true,
        compute_volume = false)
    seen = Dict{Float64, Int}()
    for pr in bf.path_records
        pr.included || continue
        vtxs = pr.vertex_indices; ros = pr.exact_profile
        length(vtxs) == length(ros) || continue
        for (v, ro) in zip(vtxs, ros)
            isfinite(ro) || continue
            rr = round(Float64(ro), digits = 3)
            haskey(seen, rr) || (seen[rr] = v)
        end
    end
    rungs = sort(collect(keys(seen)))
    regimes = [Dict("ro" => r, "vertex_idx" => seen[r],
                    "dominance" => string.(show_dominant_condition(model, seen[r]))) for r in rungs]
    # Representative (richest) feasible path: the ordered regime sequence + per-regime RO.
    # Its consecutive regimes are the transitions a "threshold" can be placed at.
    best = nothing; best_n = -1
    for pr in bf.path_records
        pr.included || continue
        vtxs = pr.vertex_indices; ros = pr.exact_profile
        length(vtxs) == length(ros) || continue
        nd = length(unique(r for r in ros if isfinite(r)))
        if nd > best_n; best_n = nd; best = (vtxs, ros); end
    end
    path = best === nothing ? Any[] :
        [Dict("vertex_idx" => v, "ro" => (isfinite(r) ? round(Float64(r), digits = 3) : nothing))
         for (v, r) in zip(best[1], best[2])]
    return Dict("ladder" => rungs, "regimes" => regimes, "path" => path)
end

# POST /api/placer_menu — { rules|session, input_sym, output_sym } -> { ladder, regimes }
function handle_placer_menu(req)
    body = read_json(req)
    rules = if _raw_haskey(body, :rules)
        String.(collect(_raw_get(body, :rules, String[])))
    else
        bundle, err = _resolve_bundle_or_response(body)
        err === nothing || return err
        String.(collect(bundle["rules"]))
    end
    isempty(rules) && return error_response("No reactions: provide `rules` or a resolvable model"; status = 400)
    _raw_haskey(body, :input_sym)  || return error_response("`input_sym` is required"; status = 400)
    _raw_haskey(body, :output_sym) || return error_response("`output_sym` is required"; status = 400)
    res = try
        placer_menu(rules, Symbol(_raw_get(body, :input_sym, "")), Symbol(_raw_get(body, :output_sym, "")))
    catch e
        return error_response("ParameterPlacer menu failed: $(sprint(showerror, e))"; status = 400)
    end
    return json_response(res)
end

# POST /api/placer_curve — { rules|session, input_sym, output_sym, kd:[...], totals:{}|null }
# -> dose-response curve at the given kd (for the live-tuning slider). Reuses placer_dose_response.
function handle_placer_curve(req)
    body = read_json(req)
    rules = if _raw_haskey(body, :rules)
        String.(collect(_raw_get(body, :rules, String[])))
    else
        bundle, err = _resolve_bundle_or_response(body)
        err === nothing || return err
        String.(collect(bundle["rules"]))
    end
    isempty(rules) && return error_response("No reactions: provide `rules` or a resolvable model"; status = 400)
    _raw_haskey(body, :input_sym)  || return error_response("`input_sym` is required"; status = 400)
    _raw_haskey(body, :output_sym) || return error_response("`output_sym` is required"; status = 400)
    _raw_haskey(body, :kd)         || return error_response("`kd` is required"; status = 400)
    input_sym  = Symbol(_raw_get(body, :input_sym, ""))
    output_sym = String(_raw_get(body, :output_sym, ""))
    kd = Float64.(collect(_raw_get(body, :kd, Float64[])))
    totals = Dict{Symbol, Float64}()
    tw = _raw_get(body, :totals, nothing)
    if tw !== nothing
        for (k, v) in pairs(tw); totals[Symbol(k)] = Float64(v); end
    end
    curve = try
        placer_dose_response(rules, kd, totals, input_sym, output_sym)
    catch e
        return error_response("Dose-response curve failed: $(sprint(showerror, e))"; status = 400)
    end
    return json_response(curve)
end

# Place a regime TRANSITION (threshold / EC50 / knee) at a target input dose, by
# pinning the input coordinate AND sitting on the from->to interface hyperplane,
# then taking an interior point of the from-regime cell. Ported from the standalone
# place_threshold (uses BindingAndCatalysis.get_interface_qK). Verifies by forward scan.
function placer_threshold(rules::Vector{String}, input_sym, output_sym,
                          from_idx::Int, to_idx::Int, target_input_value::Real; tol_dec::Real = 0.2)
    input_sym = Symbol(input_sym); output_sym = Symbol(output_sym)
    model, _, _, _ = build_model(rules, fill(1.0, length(rules)))
    find_all_vertices!(model)
    SISOPaths(model, input_sym)   # populate oriented qK interfaces along the input axis
    input_idx  = locate_sym_qK(model, input_sym); input_idx === nothing && error("Unknown input: $input_sym")
    output_idx = locate_sym_x(model, output_sym); output_idx === nothing && error("Unknown output: $output_sym")
    n = model.n
    dir, c = try
        BindingAndCatalysis.get_interface_qK(model, from_idx, to_idx)
    catch
        BindingAndCatalysis.get_interface_direct(model, from_idx, to_idx)
    end
    dirv = collect(Float64.(Vector(dir)))
    logtgt = log10(Float64(target_input_value))
    e_input = zeros(Float64, n); e_input[input_idx] = 1.0
    # equality rows (get_polyhedron builds hrep(-C,C0); equality a·x==rhs -> row a, C0 -rhs)
    C  = vcat(reshape(e_input, 1, n), reshape(dirv, 1, n))
    C0 = Float64[-logtgt, c]
    vC, vC0, vnull = get_C_C0_nullity_qK(model, from_idx)
    vC = Matrix{Float64}(vC); vC0 = Vector{Float64}(vC0)
    if vnull > 0
        eqC = vcat(C, vC[1:vnull, :]); eqC0 = vcat(C0, vC0[1:vnull])
        ineqC = vC[vnull+1:end, :];    ineqC0 = vC0[vnull+1:end]
    else
        eqC = C; eqC0 = C0; ineqC = vC; ineqC0 = vC0
    end
    poly = get_polyhedron(vcat(eqC, ineqC), vcat(eqC0, ineqC0), size(eqC, 1))
    isempty(poly) && return (; feasible = false,
        reason = "Threshold infeasible: pinning input=$target_input_value on the " *
                 "$from_idx→$to_idx interface empties the from-regime cell.")
    logqK = collect(Float64.(get_one_inner_point(poly)))
    kd, totals = _placer_split_log_qK(model, logqK)
    # verify: scan the input across decades at the solved background; find the from->to breakpoint
    fixed = copy(logqK); deleteat!(fixed, input_idx)
    onehot = zeros(Float64, length(x_sym(model))); onehot[output_idx] = 1.0
    rng = collect(range(logtgt - 2.0, logtgt + 2.0; length = 401))
    _, _, regimes = scan_parameter_1d(model, input_idx, rng, [onehot], fixed;
        input_logspace = true, output_logspace = true)
    bp_log = NaN
    for i in 2:length(regimes)
        if regimes[i] == to_idx && regimes[i-1] == from_idx
            bp_log = (rng[i] + rng[i-1]) / 2; break
        end
    end
    pass = isfinite(bp_log) && abs(bp_log - logtgt) <= tol_dec
    return (; feasible = true, kd, totals, target_input = Float64(target_input_value),
            measured_breakpoint = isfinite(bp_log) ? 10.0^bp_log : NaN,
            pass, dominance_ordering = string.(show_dominant_condition(model, to_idx)))
end

# POST /api/placer_threshold — { rules|session, input_sym, output_sym, from_idx, to_idx, target_input }
function handle_placer_threshold(req)
    body = read_json(req)
    rules = if _raw_haskey(body, :rules)
        String.(collect(_raw_get(body, :rules, String[])))
    else
        bundle, err = _resolve_bundle_or_response(body)
        err === nothing || return err
        String.(collect(bundle["rules"]))
    end
    isempty(rules) && return error_response("No reactions"; status = 400)
    for k in (:input_sym, :output_sym, :from_idx, :to_idx)
        _raw_haskey(body, k) || return error_response("`$k` is required"; status = 400)
    end
    input_sym  = Symbol(_raw_get(body, :input_sym, ""))
    output_sym = Symbol(_raw_get(body, :output_sym, ""))
    from_idx   = Int(_raw_get(body, :from_idx, 0))
    to_idx     = Int(_raw_get(body, :to_idx, 0))
    target     = Float64(_raw_get(body, :target_input, 1.0))
    res = try
        placer_threshold(rules, input_sym, output_sym, from_idx, to_idx, target)
    catch e
        return error_response("Threshold placement failed: $(sprint(showerror, e))"; status = 400)
    end
    res.feasible || return json_response(Dict("error" => res.reason))
    kd_vec = collect(Float64.(res.kd))
    curve = try
        placer_dose_response(rules, kd_vec, res.totals, input_sym, String(output_sym))
    catch e
        return error_response("Dose-response curve failed: $(sprint(showerror, e))"; status = 400)
    end
    return json_response(Dict(
        "kd"                  => kd_vec,
        "totals"              => Dict(string(k) => v for (k, v) in res.totals),
        "target_input"        => res.target_input,
        "measured_breakpoint" => json_safe_real(res.measured_breakpoint),
        "pass"                => res.pass,
        "dominance_ordering"  => res.dominance_ordering,
        "dose_response_curve" => curve,
    ))
end

# ════════ REALIZE THE WHOLE REGIME PROGRAM (the design target is a program) ════════
# place_parameters lands ONE regime (one slope). A design target is a regime
# PROGRAM — the ordered sequence of slopes the dose-response walks. The network was
# selected because its asymptotic itinerary already matches that program; realizing
# it = solving a Kd ORDERING (the background kd + non-input totals) that lays the
# whole transition sequence on a monotone input-dose ladder, so the swept response
# traverses the program with separated breakpoints. This is the "no-sampling via Kd
# ordering" step: each consecutive regime pair has an interface hyperplane
# dir·logqK = c (get_interface_qK); the breakpoint dose is where it is crossed as the
# input sweeps, which is LINEAR in the background — so placing the k transitions at a
# monotone dose ladder is one linear solve. Forward-verified by sweeping the input.
function _placer_segment_ros(pvals::Vector{Float64}, otraj::Vector{Float64},
                             regimes::Vector{Int})
    # Walk the swept regime labels; for each maximal run, measure the local log-log
    # slope (reaction order) at its interior and record the run + its breakpoint dose.
    segs = Vector{NamedTuple{(:vtx, :ro, :i0, :i1), Tuple{Int, Float64, Int, Int}}}()
    i = 1; N = length(regimes)
    while i <= N
        j = i
        while j < N && regimes[j+1] == regimes[i]; j += 1; end
        # interior central-difference slope (skip the 1-2 endpoints touching the knee)
        a = clamp(i + 1, 1, N); b = clamp(j - 1, 1, N)
        ro = NaN
        if b > a
            num = otraj[b] - otraj[a]; den = pvals[b] - pvals[a]
            ro = den == 0 ? NaN : num / den
        elseif j > i
            num = otraj[j] - otraj[i]; den = pvals[j] - pvals[i]
            ro = den == 0 ? NaN : num / den
        end
        push!(segs, (; vtx = regimes[i], ro = ro, i0 = i, i1 = j))
        i = j + 1
    end
    return segs
end
function _collapse_signs(ros)
    out = Char[]
    for r in ros
        (isfinite(r) && abs(r) > 0.05) || continue
        c = r > 0 ? '+' : '-'
        (isempty(out) || out[end] != c) && push!(out, c)
    end
    return String(out)
end
function placer_realize_program(rules::Vector{String}, input_sym, output_sym;
                                kd_bounds::Tuple{<:Real, <:Real} = (-3.0, 3.0),
                                decade_gap::Real = 2.0)
    input_sym = Symbol(input_sym); output_sym = Symbol(output_sym)
    model, _, _, _ = build_model(rules, fill(1.0, length(rules)))
    find_all_vertices!(model)
    input_idx  = locate_sym_qK(model, input_sym);  input_idx  === nothing && error("Unknown input $input_sym")
    output_idx = locate_sym_x(model, output_sym);  output_idx === nothing && error("Unknown output $output_sym")
    siso = SISOPaths(model, input_sym)
    bf = get_behavior_families(siso; observe_x = output_sym, path_scope = :feasible,
        deduplicate = false, keep_singular = true, keep_nonasymptotic = true, compute_volume = false)
    best = nothing; best_n = -1
    for pr in bf.path_records
        pr.included || continue
        vtxs = pr.vertex_indices; ros = pr.exact_profile
        length(vtxs) == length(ros) || continue
        nd = length(unique(r for r in ros if isfinite(r)))
        if nd > best_n; best_n = nd; best = (collect(Int, vtxs), collect(Float64, ros)); end
    end
    best === nothing && error("No feasible regime path for output $output_sym")
    vtxs, ros = best
    m = length(vtxs)
    m >= 2 || error("This output has a single regime — use a single-RO solve, not a program")

    n = model.n
    k = m - 1
    # target breakpoint log10-doses: a monotone ladder centred at 0 (the Kd ordering).
    τ = [ (i - (k + 1) / 2) * Float64(decade_gap) for i in 1:k ]
    bg_idx = [j for j in 1:n if j != input_idx]
    A = zeros(Float64, k, length(bg_idx)); bvec = zeros(Float64, k); ok = trues(k)
    for i in 1:k
        from_i = vtxs[i]; to_i = vtxs[i + 1]
        dir, c = try
            BindingAndCatalysis.get_interface_qK(model, from_i, to_i)
        catch
            try BindingAndCatalysis.get_interface_direct(model, from_i, to_i) catch; (nothing, nothing) end
        end
        if dir === nothing; ok[i] = false; continue; end
        dirv = collect(Float64.(Vector(dir)))
        di = dirv[input_idx]
        if abs(di) < 1e-9; ok[i] = false; continue; end   # this interface isn't moved by the input
        # Normalise the row by the input coefficient so least-squares minimises the
        # actual breakpoint-POSITION error |log10(dose_i) - τ_i| (decades), not the
        # raw hyperplane residual — this is what spreads the transitions on the ladder.
        bvec[i] = Float64(c) / di - τ[i]
        for (col, j) in enumerate(bg_idx); A[i, col] = dirv[j] / di; end
    end
    rows = findall(ok)
    isempty(rows) && error("No placeable interfaces along the program path")
    z = A[rows, :] \ bvec[rows]   # least-squares: monotone-ladder Kd ordering
    logqK = zeros(Float64, n)
    for (col, j) in enumerate(bg_idx); logqK[j] = z[col]; end
    logqK[input_idx] = 0.0
    lo, hi = Float64(kd_bounds[1]), Float64(kd_bounds[2])
    for j in (model.d + 1):(model.d + model.r); logqK[j] = clamp(logqK[j], lo, hi); end
    kd, totals = _placer_split_log_qK(model, logqK)

    # Forward-verify: sweep the input across the full ladder window, read the realized
    # regime sequence + per-segment reaction orders + breakpoint doses.
    span = max((k + 2) * Float64(decade_gap), 6.0)
    rng = collect(range(-span / 2, span / 2; length = 700))
    fixed = copy(logqK); deleteat!(fixed, input_idx)
    coeffs = parse_linear_combination(model, String(output_sym))
    pvals, otrajm, regimes = scan_parameter_1d(model, input_idx, rng, [coeffs], fixed;
        input_logspace = true, output_logspace = true)
    otraj = vec(collect(Float64.(otrajm)))
    regv  = Int.(collect(regimes))
    segs = _placer_segment_ros(pvals, otraj, regv)
    # Keep only segments that occupy a real input interval (≥0.4 decade) — a genuine
    # asymptotic plateau, not a transient sliver between clustered breakpoints.
    WIDTH_MIN = 0.4
    wide = [s for s in segs if (pvals[s.i1] - pvals[s.i0]) >= WIDTH_MIN]
    isempty(wide) && (wide = segs)
    measured_ros = [s.ro for s in wide]
    breakpoints = Float64[]
    for s in wide[2:end]; push!(breakpoints, 10.0 ^ pvals[s.i0]); end

    target_finite = [round(r, digits = 3) for r in ros if isfinite(r)]
    measured_signs = _collapse_signs(measured_ros)
    target_signs   = _collapse_signs(target_finite)
    pass = measured_signs == target_signs && !isempty(target_signs)

    return (; feasible = true, kd, totals, log_qK = logqK,
            target_program = target_finite, measured_program = [round(r, digits = 3) for r in measured_ros if isfinite(r)],
            target_signs, measured_signs, breakpoints, pass,
            dominance_ordering = string.(show_dominant_condition(model, vtxs[end])),
            n_transitions = k, n_placed = length(rows))
end

# POST /api/placer_realize_program — { rules|session, input_sym, output_sym, kd_bounds?:[lo,hi] }
function handle_placer_realize_program(req)
    body = read_json(req)
    rules = if _raw_haskey(body, :rules)
        String.(collect(_raw_get(body, :rules, String[])))
    else
        bundle, err = _resolve_bundle_or_response(body)
        err === nothing || return err
        String.(collect(bundle["rules"]))
    end
    isempty(rules) && return error_response("No reactions: provide `rules` or a resolvable model"; status = 400)
    for k in (:input_sym, :output_sym)
        _raw_haskey(body, k) || return error_response("`$k` is required"; status = 400)
    end
    input_sym  = Symbol(_raw_get(body, :input_sym, ""))
    output_sym = Symbol(_raw_get(body, :output_sym, ""))
    kdb = _raw_get(body, :kd_bounds, nothing)
    kd_bounds = (kdb === nothing) ? (-3.0, 3.0) : (Float64(kdb[1]), Float64(kdb[2]))
    res = try
        placer_realize_program(rules, input_sym, output_sym; kd_bounds = kd_bounds)
    catch e
        return error_response("Program realization failed: $(sprint(showerror, e))"; status = 400)
    end
    kd_vec = collect(Float64.(res.kd))
    curve = try
        placer_dose_response(rules, kd_vec, res.totals, input_sym, String(output_sym))
    catch e
        return error_response("Dose-response curve failed: $(sprint(showerror, e))"; status = 400)
    end
    return json_response(Dict(
        "kd"                 => kd_vec,
        "totals"             => Dict(string(k) => v for (k, v) in res.totals),
        "target_program"     => [json_safe_real(x) for x in res.target_program],
        "measured_program"   => [json_safe_real(x) for x in res.measured_program],
        "target_signs"       => res.target_signs,
        "measured_signs"     => res.measured_signs,
        "breakpoints"        => [json_safe_real(x) for x in res.breakpoints],
        "pass"               => res.pass,
        "n_transitions"      => res.n_transitions,
        "n_placed"           => res.n_placed,
        "dominance_ordering" => res.dominance_ordering,
        "dose_response_curve" => curve,
    ))
end

# ════════ DESIGN PIPELINE: target → designable? → screen → minimal ════════
# Query the bundled enumerated atlas (slices.jsonl.gz, the complete d≤4/μ≤5 product)
# for networks whose reaction-order program matches a target, and return the
# Pareto-minimal (d,r,μ). `design_screen` adds a tunability-aware candidate
# screening layer without folding model building or placement into this node.
const _DESIGN_INDEX = Ref{Any}(nothing)
const DESIGN_SCREEN_SCHEMA_VERSION = "bne-design-screen/v0.1.0"

function _design_collapse(progstr::AbstractString)
    toks = Float64[]
    for t in split(progstr, r"→|->|,")
        s = strip(t)
        (isempty(s) || occursin("Inf", s) || occursin("NaN", s)) && continue
        v = tryparse(Float64, s); v === nothing && continue
        push!(toks, round(v, digits = 3))
    end
    out = Float64[]
    for v in toks; (isempty(out) || out[end] != v) && push!(out, v); end
    return out
end
function _design_sign(prog::Vector{Float64})
    out = Char[]
    for v in prog
        v == 0 && continue
        c = v > 0 ? '+' : '-'
        (isempty(out) || out[end] != c) && push!(out, c)
    end
    return String(out)
end

function _design_species_labels(sym::AbstractString)
    text = strip(String(sym))
    isempty(text) && return String[]
    if startswith(text, "C_")
        return [String(label) for label in split(text[3:end], "_")]
    end
    return [text]
end

function _design_species_symbol(labels::AbstractVector{<:AbstractString})
    sorted_labels = sort(String.(labels))
    isempty(sorted_labels) && return ""
    length(sorted_labels) == 1 && return first(sorted_labels)
    return "C_" * join(sorted_labels, "_")
end

function _design_label_index(label::AbstractString)
    text = String(label)
    if length(text) == 1
        ch = only(text)
        ('A' <= ch <= 'Z') && return Int(ch - 'A') + 1
    end
    if startswith(text, "S")
        idx = tryparse(Int, text[2:end])
        idx === nothing || return idx
    end
    return nothing
end

function _design_canonical_species(nid::AbstractString)
    species = Set{String}()
    max_idx = 0
    for reaction in split(String(nid), "|")
        parts = split(reaction, "<->")
        length(parts) == 2 || continue
        for side in parts
            for term in _atlas_sqlite_parse_canonical_side(side)
                isempty(term) && continue
                max_idx = max(max_idx, maximum(term))
                push!(species, _design_species_symbol([_base_species_label(Int(idx)) for idx in term]))
            end
        end
    end
    return species, max_idx
end

function _design_symbol_max_index(symbols)
    max_idx = 0
    for sym in symbols
        for label in _design_species_labels(String(sym))
            idx = _design_label_index(label)
            idx === nothing && continue
            max_idx = max(max_idx, idx)
        end
    end
    return max_idx
end

function _design_permutation_orders(n::Int)
    n <= 0 && return [Int[]]
    n > 7 && return [collect(1:n)]
    out = Vector{Vector{Int}}()
    used = falses(n)
    current = Int[]
    function visit!()
        if length(current) == n
            push!(out, copy(current))
            return
        end
        for idx in 1:n
            used[idx] && continue
            used[idx] = true
            push!(current, idx)
            visit!()
            pop!(current)
            used[idx] = false
        end
    end
    visit!()
    return out
end

function _design_apply_species_map(label_map::Dict{String,String}, sym::AbstractString)
    labels = _design_species_labels(sym)
    isempty(labels) && return String(sym)
    return _design_species_symbol([get(label_map, label, label) for label in labels])
end

function _design_apply_total_map(label_map::Dict{String,String}, sym::AbstractString)
    text = String(sym)
    m = match(r"^t(.+)$", text)
    m === nothing && return text
    label = String(m.captures[1])
    return "t" * get(label_map, label, label)
end

function _design_symbol_map_for_nid(nid::AbstractString, raw_outputs::Set{String})
    canonical_species, canonical_max = _design_canonical_species(nid)
    isempty(canonical_species) && return Dict{String,String}()
    n = max(canonical_max, _design_symbol_max_index(raw_outputs))
    labels = [_base_species_label(idx) for idx in 1:n]
    best_map = Dict(label => label for label in labels)
    best_score = -1
    best_extra = typemax(Int)
    best_moves = typemax(Int)
    for order in _design_permutation_orders(n)
        label_map = Dict(labels[idx] => labels[order[idx]] for idx in 1:n)
        mapped_outputs = Set(_design_apply_species_map(label_map, out) for out in raw_outputs)
        score = count(out -> out in canonical_species, mapped_outputs)
        extra = count(out -> !(out in canonical_species), mapped_outputs)
        moves = count(idx -> order[idx] != idx, 1:n)
        if score > best_score ||
           (score == best_score && extra < best_extra) ||
           (score == best_score && extra == best_extra && moves < best_moves)
            best_map = label_map
            best_score = score
            best_extra = extra
            best_moves = moves
        end
    end
    return best_map
end

function _load_design_index()
    _DESIGN_INDEX[] === nothing || return _DESIGN_INDEX[]
    # Tracked new-sign atlas (147,081 slices, carries `base_motifs`). The old
    # webapp/data copy is stale (146,845, old singular-sign); BNE_DESIGN_INDEX overrides.
    path = get(ENV, "BNE_DESIGN_INDEX",
               normpath(joinpath(@__DIR__, "..", "..", "paper_rop_periodic_table",
                                 "data", "slices.jsonl.gz")))
    isfile(path) || error("design index (slices.jsonl.gz) not found at $path")
    raw_recs = NamedTuple[]
    raw_outputs_by_nid = Dict{String, Set{String}}()
    open(`gzip -dc $path`) do io
        for line in eachline(io)
            isempty(strip(line)) && continue
            j = JSON3.read(line)
            ex = get(j, :exact, nothing); ex === nothing && continue
            progs = Vector{Float64}[]
            for p in ex
                cp = _design_collapse(String(p)); isempty(cp) || push!(progs, cp)
            end
            isempty(progs) && continue
            bms = get(j, :base_motifs, nothing)
            motifs = bms === nothing ? Set{String}() : Set(String.(bms))
            nid = String(get(j, :nid, ""))
            inp = String(get(j, :inp, ""))
            out = String(get(j, :out, ""))
            push!(raw_recs, (; d = Int(j.d), r = Int(j.r), mu = Int(j.mu),
                              nid = nid, inp = inp, out = out,
                              signs = Set(_design_sign(p) for p in progs), exacts = Set(progs),
                              motifs = motifs,
                              feasible_paths = Int(get(j, :feasible_paths, 0)),
                              total_paths = Int(get(j, :total_paths, 0)),
                              n_species = Int(get(j, :n_species, 0)),
                              n_complexes = Int(get(j, :n_complexes, 0)),
                              max_complex_size = Int(get(j, :max_complex_size, 0)),
                              assembly_depth = Int(get(j, :assembly_depth, 0)),
                              uses_homomer = Int(get(j, :uses_homomer, 0)),
                              uses_complex_growth = Int(get(j, :uses_complex_growth, 0)),
                              uses_higher_order_template = Int(get(j, :uses_higher_order_template, 0)),
                              sl_nswitch = Int(get(j, :sl_nswitch, 0)),
                              sl_absro = Float64(get(j, :sl_absro, 0.0)),
                              vol_sum = Float64(get(j, :vol_sum, 0.0)),
                              vol_max = Float64(get(j, :vol_max, 0.0)),
                              robust_paths = Int(get(j, :robust_paths, 0))))
            if !isempty(nid) && !isempty(out)
                push!(get!(raw_outputs_by_nid, nid, Set{String}()), out)
            end
        end
    end
    symbol_maps = Dict(nid => _design_symbol_map_for_nid(nid, outs)
                       for (nid, outs) in raw_outputs_by_nid)
    recs = NamedTuple[]
    for rec in raw_recs
        label_map = get(symbol_maps, rec.nid, Dict{String,String}())
        push!(recs, (; d = rec.d, r = rec.r, mu = rec.mu,
                      nid = rec.nid,
                      inp = _design_apply_total_map(label_map, rec.inp),
                      out = _design_apply_species_map(label_map, rec.out),
                      signs = rec.signs, exacts = rec.exacts, motifs = rec.motifs,
                      feasible_paths = rec.feasible_paths, total_paths = rec.total_paths,
                      n_species = rec.n_species, n_complexes = rec.n_complexes,
                      max_complex_size = rec.max_complex_size,
                      assembly_depth = rec.assembly_depth,
                      uses_homomer = rec.uses_homomer,
                      uses_complex_growth = rec.uses_complex_growth,
                      uses_higher_order_template = rec.uses_higher_order_template,
                      sl_nswitch = rec.sl_nswitch, sl_absro = rec.sl_absro,
                      vol_sum = rec.vol_sum, vol_max = rec.vol_max,
                      robust_paths = rec.robust_paths))
    end
    _DESIGN_INDEX[] = recs
    return recs
end
function _design_pareto(cells)
    cs = sort(collect(Set(cells)))
    [c for c in cs if !any(o != c && o[1] <= c[1] && o[2] <= c[2] && o[3] <= c[3] for o in cs)]
end

function _design_normalize_design_target(target_kind, target)
    target === nothing && error("`target` is required")
    kind = String(target_kind)
    kind in ("sign", "exact", "label") ||
        error("target_kind must be one of `sign`, `exact`, or `label`")
    if kind == "exact"
        target isa AbstractString && error("exact target must be an array of numbers")
        vals = Float64[]
        for value in collect(target)
            (value isa Bool || !(value isa Real)) &&
                error("exact target values must be finite non-Bool numbers")
            f = Float64(value)
            isfinite(f) || error("exact target values must be finite non-Bool numbers")
            push!(vals, f)
        end
        isempty(vals) && error("exact target must contain at least one number")
        return kind, vals
    else
        text = strip(String(target))
        isempty(text) && error("`target` must not be empty")
        return kind, text
    end
end

function _design_matches_normalized(target_kind::AbstractString, target)
    idx = _load_design_index()
    matched = NamedTuple[]
    if target_kind == "label"
        tgt = String(target)
        for rec in idx; (tgt in rec.motifs) && push!(matched, rec); end
    elseif target_kind == "sign"
        tgt = String(target)
        for rec in idx; (tgt in rec.signs) && push!(matched, rec); end
    else
        tgt = Float64[round(Float64(x), digits = 3) for x in target]
        for rec in idx; any(==(tgt), rec.exacts) && push!(matched, rec); end
    end
    return matched
end

function _design_matches(target_kind::AbstractString, target)
    kind, normalized_target = _design_normalize_design_target(target_kind, target)
    return _design_matches_normalized(kind, normalized_target)
end

function _design_search_response(matched, cells)
    pm = _design_pareto(cells)
    minimal = Dict[]
    for c in pm
        nets = [Dict("nid" => m.nid, "inp" => m.inp, "out" => m.out)
                for m in matched if (m.d, m.r, m.mu) == c]
        push!(minimal, Dict("d" => c[1], "r" => c[2], "mu" => c[3],
                            "networks" => nets[1:min(length(nets), 8)]))
    end
    return Dict("designable" => !isempty(matched), "n_matches" => length(matched),
                "minimal" => minimal,
                "all_cells" => sort([[c[1], c[2], c[3]] for c in Set(cells)]))
end

# target_kind: "sign" (e.g. "+-+"), "exact" (Vector of numbers, e.g. [1.5,0,-1]),
# or "label" (a behavior base-motif, e.g. "biphasic_peak" — matched against each
# slice's precomputed `base_motifs`, so no classifier is needed).
function design_search(target_kind::AbstractString, target)
    kind, normalized_target = _design_normalize_design_target(target_kind, target)
    matched = _design_matches_normalized(kind, normalized_target)
    cells = [(m.d, m.r, m.mu) for m in matched]
    return _design_search_response(matched, cells)
end

function _design_float(raw, key::Symbol, default::Real)
    value = _raw_get(raw, key, default)
    value === nothing && return Float64(default)
    return Float64(value)
end

function _design_int(raw, key::Symbol, default::Integer)
    value = _raw_get(raw, key, default)
    value === nothing && return Int(default)
    return Int(value)
end

function _design_dict_or_empty(raw)
    raw === nothing && return Dict{String, Any}()
    try
        keys(raw)
        return raw
    catch
        return Dict{String, Any}()
    end
end

function _design_bounds(raw, key::Symbol, default::Tuple{Float64, Float64})
    value = _raw_get(raw, key, nothing)
    value === nothing && return default
    vals = collect(value)
    length(vals) == 2 || error("`$key` must have two values")
    lo, hi = Float64(vals[1]), Float64(vals[2])
    lo < hi || error("`$key` lower bound must be < upper bound")
    return (lo, hi)
end

function _design_candidate_key(rec)
    return string(rec.nid, "::", rec.inp, "::", rec.out)
end

function _design_better_record(a, b)
    a_score = (Float64(a.vol_max), Int(a.robust_paths), Float64(a.vol_sum), -Int(a.r), -Int(a.d))
    b_score = (Float64(b.vol_max), Int(b.robust_paths), Float64(b.vol_sum), -Int(b.r), -Int(b.d))
    return a_score > b_score ? a : b
end

function _design_unique_records(records)
    by_key = Dict{String, Any}()
    for rec in records
        key = _design_candidate_key(rec)
        by_key[key] = haskey(by_key, key) ? _design_better_record(rec, by_key[key]) : rec
    end
    return [by_key[key] for key in sort(collect(keys(by_key)))]
end

function _design_near_minimal(rec, pareto_cells, max_extra_d::Int, max_extra_r::Int, max_extra_mu::Int)
    isempty(pareto_cells) && return false
    any(c -> rec.d <= c[1] + max_extra_d &&
             rec.r <= c[2] + max_extra_r &&
             rec.mu <= c[3] + max_extra_mu, pareto_cells)
end

function _design_total_symbols(d::Integer)
    return ["t" * _base_species_label(idx) for idx in 1:Int(d)]
end

function _design_complex_token(term)
    labels = [_base_species_label(Int(idx)) for idx in term]
    isempty(labels) && return ""
    length(labels) == 1 && return first(labels)
    return "C_" * join(labels, "_")
end

function _design_side_to_rule_species(side::AbstractString)
    terms = _atlas_sqlite_parse_canonical_side(side)
    return join([_design_complex_token(term) for term in terms], " + ")
end

function _design_nid_to_rules(nid::AbstractString)
    rules = String[]
    for reaction in split(String(nid), "|")
        parts = split(reaction, "<->")
        length(parts) == 2 || error("Cannot parse reaction in nid: $reaction")
        push!(rules, string(_design_side_to_rule_species(parts[1]), " <-> ",
                            _design_side_to_rule_species(parts[2])))
    end
    return rules
end

function _design_target_transition_count(rec)
    longest = 1
    for prog in rec.exacts
        longest = max(longest, length(prog))
    end
    return max(Int(rec.sl_nswitch), longest - 1, 1)
end

function _design_screen_card(rec, pareto_cell_set, tuning, ranking_policy)
    kd_bounds = _design_bounds(tuning, :kd_bounds, (-3.0, 3.0))
    total_bounds = _design_bounds(tuning, :total_bounds, (-3.0, 3.0))
    min_dynamic_range = _design_float(tuning, :min_dynamic_range, 0.0)
    min_transition_spacing = _design_float(tuning, :min_transition_spacing_decades, 0.0)
    min_margin_radius = _design_float(tuning, :min_chebyshev_radius, 0.0)

    kd_span = kd_bounds[2] - kd_bounds[1]
    transition_count = _design_target_transition_count(rec)
    transition_spacing = kd_span / max(transition_count, 1)
    volume_radius = rec.vol_max > 0 ? rec.vol_max^(1 / max(rec.r, 1)) : 0.0
    cheb_radius = min(kd_span / 2, transition_spacing / 2, volume_radius)
    dynamic_range = max(0.0, Float64(rec.sl_absro)) + log10(1 + max(Int(rec.robust_paths), 0))
    tunable_volume = max(Float64(rec.vol_max), 0.0)
    volume_sum = max(Float64(rec.vol_sum), 0.0)
    sampled_robustness = max(Int(rec.robust_paths), 0) / max(max(Int(rec.robust_paths), Int(rec.total_paths)), 1)
    parameter_extremeness = rec.r / max(kd_span, eps(Float64))
    condition_number = 1.0 + rec.r / max(rec.robust_paths, 1) + transition_count / max(kd_span, eps(Float64))
    sensitivity = 1.0 / max(transition_spacing, eps(Float64))

    active_failures = String[]
    dynamic_range < min_dynamic_range && push!(active_failures, "dynamic_range_below_min")
    transition_spacing < min_transition_spacing && push!(active_failures, "transition_spacing_below_min")
    cheb_radius < min_margin_radius && push!(active_failures, "chebyshev_radius_below_min")
    rec.vol_max <= 0 && push!(active_failures, "no_atlas_volume_proxy")
    pass = isempty(active_failures)

    radius_norm = clamp(cheb_radius / max(kd_span / 2, eps(Float64)), 0.0, 1.0)
    volume_norm = clamp(tunable_volume, 0.0, 1.0)
    spacing_norm = clamp(transition_spacing / max(kd_span, eps(Float64)), 0.0, 1.0)
    dynamic_norm = clamp(dynamic_range / max(min_dynamic_range, 1.0), 0.0, 1.0)
    complexity_penalty = clamp(0.03 * rec.r + 0.02 * rec.d + 0.02 * rec.mu, 0.0, 0.5)
    score = clamp(0.34 * radius_norm + 0.22 * volume_norm + 0.16 * sampled_robustness +
                  0.12 * spacing_norm + 0.10 * dynamic_norm - complexity_penalty, 0.0, 1.0)

    totals = Dict(sym => 1.0 for sym in _design_total_symbols(rec.d))
    kd = fill(1.0, rec.r)
    log_qK = fill(0.0, rec.d + rec.r)
    minimal = (rec.d, rec.r, rec.mu) in pareto_cell_set
    grade = rec.vol_max > 0 ? "atlas-volume-proxy" : "screened-surrogate"

    return Dict(
        "schema_version" => "bne-designability-card/v0.1.0",
        "card_id" => bytes2hex(sha256(_design_candidate_key(rec)))[1:16],
        "nid" => rec.nid,
        "inp" => rec.inp,
        "out" => rec.out,
        "minimal" => minimal,
        "pass" => pass,
        "tunability_score" => round(score; digits=4),
        "certificate_grade" => grade,
        "screen_status" => pass ? "proxy_pass" : "proxy_near_miss",
        "reason" => pass ? "proxy screen pass; downstream Placer must compute exact parameter placement" :
                           "candidate retained as a certificate or near-miss; inspect active_failures",
        "complexity" => Dict(
            "d" => rec.d, "r" => rec.r, "mu" => rec.mu,
            "n_species" => rec.n_species, "n_complexes" => rec.n_complexes,
            "max_complex_size" => rec.max_complex_size,
            "assembly_depth" => rec.assembly_depth,
            "uses_homomer" => rec.uses_homomer == 1,
            "uses_complex_growth" => rec.uses_complex_growth == 1,
            "uses_higher_order_template" => rec.uses_higher_order_template == 1,
        ),
        "constraints" => Dict(
            "kd_bounds" => [kd_bounds[1], kd_bounds[2]],
            "total_bounds" => [total_bounds[1], total_bounds[2]],
            "min_dynamic_range" => min_dynamic_range,
            "min_transition_spacing_decades" => min_transition_spacing,
            "min_chebyshev_radius" => min_margin_radius,
            "supported_handles" => ["reaction_order_program", "kd_bounds", "atlas_volume", "robust_path_count"],
            "unsupported_handles" => ["finite_dose_peak_uniqueness", "multi_input_surface_exact_chebyshev", "temporal_dynamics"],
            "supported_constraints" => Any[
                Dict("path" => "target", "kind" => "reaction_order_program",
                     "support_level" => "enforced", "stage" => "atlas_match",
                     "solver" => "design_index"),
                Dict("path" => "tuning.kd_bounds", "kind" => "parameter_box",
                     "support_level" => "proxy_only", "stage" => "screen",
                     "solver" => "design_screen_proxy"),
            ],
            "unsupported_constraints" => Any[
                Dict("path" => "temporal_dynamics", "kind" => "dynamics",
                     "support_level" => "ignored", "stage" => "screen",
                     "solver" => "none",
                     "reason" => "design_screen currently ranks equilibrium ROP candidates"),
            ],
            "bounds_intersection_verified" => false,
            "total_bounds_role" => "forwarded_to_downstream_placer_not_used_by_proxy_screen",
        ),
        "metrics" => Dict(
            "chebyshev_radius" => round(cheb_radius; digits=4),
            "chebyshev_radius_source" => "atlas_volume_radius_proxy_not_exact_feasible_region",
            "chebyshev_units" => "log10_qK_euclidean_proxy",
            "tunable_volume" => round(tunable_volume; digits=6),
            "tunable_volume_sum" => round(volume_sum; digits=6),
            "dynamic_range" => round(dynamic_range; digits=4),
            "transition_spacing" => round(transition_spacing; digits=4),
            "parameter_extremeness" => round(parameter_extremeness; digits=4),
            "condition_number" => round(condition_number; digits=4),
            "parameter_breakpoint_sensitivity" => round(sensitivity; digits=4),
            "sampled_robustness" => round(sampled_robustness; digits=4),
            "robust_path_count" => rec.robust_paths,
            "feasible_path_count" => rec.feasible_paths,
            "total_path_count" => rec.total_paths,
            "sl_nswitch" => rec.sl_nswitch,
            "sl_absro" => rec.sl_absro,
        ),
        "parameter_recommendation" => Dict(
            "theta_star" => Dict(
                "status" => "not_computed",
                "source" => "declared_box_center_seed_only",
                "placement_status" => "not_attempted",
                "verification_status" => "not_attempted",
                "source_type" => "seed",
                "source_endpoint" => "/api/design_screen",
                "bounds_verified" => false,
                "log_qK" => log_qK,
                "kd" => kd,
                "totals" => totals,
                "note" => "This is a neutral downstream seed, not a solved parameter recommendation. Exact behavior Chebyshev is computed by Placer when a linear ROP component is selected.",
            ),
            "downstream" => Dict(
                "model_builder" => true,
                "placer" => true,
                "build_and_tune_uses" => "selected recommended candidate",
            ),
        ),
        "placement_attempt" => Dict(
            "attempted" => false,
            "status" => "not_attempted",
            "endpoint" => "/api/design_screen",
            "mode" => "none",
        ),
        "certificate_stack" => Any[
            Dict("grade" => grade,
                 "scope" => "atlas_slice",
                 "soundness" => grade == "atlas-volume-proxy" ? "proxy" : "surrogate",
                 "source" => "design_index",
                 "supports_exact_placement" => false),
        ],
        "active_failures" => active_failures,
        "active_failure_details" => Any[
            Dict("code" => failure,
                 "severity" => "warning",
                 "stage" => "proxy_screen",
                 "blocks_recommendation" => true,
                 "message_agent" => failure)
            for failure in active_failures
        ],
        "agent_handoff" => Dict(
            "endpoint" => "/api/design_screen",
            "role" => "candidate_screen_result",
            "next_actions" => ["select_network", "build_model", "run_parameter_placer"],
            "agent_should_not" => ["invent_curve", "treat_proxy_radius_as_exact_linear_certificate"],
        ),
    )
end

function _design_card_sort_key(card)
    metrics = card["metrics"]
    complexity = card["complexity"]
    return (-Float64(card["tunability_score"]),
            -Float64(metrics["chebyshev_radius"]),
            -Float64(metrics["tunable_volume"]),
            Int(complexity["r"]),
            Int(complexity["d"]),
            Int(complexity["mu"]))
end

function _design_ranking_preferences(ranking_policy)
    supported = Set(["certificate_grade", "chebyshev_radius", "tunable_volume",
                     "dynamic_range", "transition_spacing", "sampled_robustness",
                     "complexity", "condition_number"])
    raw = collect(_raw_get(ranking_policy, :prefer,
        Any["certificate_grade", "chebyshev_radius", "tunable_volume", "complexity"]))
    prefs = [String(p) for p in raw if String(p) in supported]
    return isempty(prefs) ? ["certificate_grade", "chebyshev_radius", "tunable_volume", "complexity"] : prefs
end

function _design_certificate_rank(card)
    grade = String(get(card, "certificate_grade", ""))
    grade == "exact-linear" && return 3.0
    grade == "union-of-polytopes" && return 2.0
    return card["pass"] === true ? 1.0 : 0.0
end

function _design_object_keys(obj)
    obj === nothing && return String[]
    try
        return [String(k) for k in keys(obj)]
    catch
        return String[]
    end
end

function _design_spec_interpretation(target_kind::AbstractString, target, designability_spec;
                                     exact_placement=Dict{String, Any}())
    supported = Set(["target_kind", "target", "candidate_budget", "tuning", "ranking_policy"])
    raw_keys = _design_object_keys(designability_spec)
    unhandled = [key for key in raw_keys if !(key in supported)]
    attempted = Int(_raw_get(exact_placement, :attempted, 0))
    succeeded = Int(_raw_get(exact_placement, :succeeded, 0))
    scope = if attempted > 0
        "atlas reaction-order match plus budgeted exact single-RO placement for attempted candidates; multi-regime and temporal specs remain downstream"
    else
        "atlas reaction-order match plus tunability proxy ranking; exact parameter placement remains downstream unless candidate_budget enables exact placement for a supported target"
    end
    return Dict(
        "normalized_target_kind" => String(target_kind),
        "normalized_target" => target,
        "supported_clauses" => ["target_kind", "target", "candidate_budget", "tuning", "ranking_policy"],
        "unhandled_clauses" => sort(unhandled),
        "degradation" => isempty(unhandled) ?
            "none" :
            "unsupported clauses were preserved in designability_spec but not enforced by the active screen",
        "current_solver_scope" => scope,
        "exact_placement" => Dict(
            "attempted" => attempted,
            "succeeded" => succeeded,
            "scope" => "single_ro",
        ),
    )
end

function _design_card_preference_value(card, pref::AbstractString)
    metrics = card["metrics"]
    complexity = card["complexity"]
    pref == "certificate_grade" && return _design_certificate_rank(card)
    pref == "chebyshev_radius" && return Float64(metrics["chebyshev_radius"])
    pref == "tunable_volume" && return Float64(metrics["tunable_volume"])
    pref == "dynamic_range" && return Float64(metrics["dynamic_range"])
    pref == "transition_spacing" && return Float64(metrics["transition_spacing"])
    pref == "sampled_robustness" && return Float64(metrics["sampled_robustness"])
    pref == "condition_number" && return -Float64(metrics["condition_number"])
    if pref == "complexity"
        return -(Float64(complexity["r"]) + Float64(complexity["d"]) + Float64(complexity["mu"]))
    end
    return 0.0
end

function _design_exact_single_ro(target_kind::AbstractString, target)
    target_kind == "exact" || return nothing
    vals = collect(target)
    length(vals) == 1 || return nothing
    return Float64(first(vals))
end

function _design_string_totals(totals)
    return Dict(String(k) => Float64(v) for (k, v) in pairs(totals))
end

function _design_exact_placement_budget(candidate_budget)
    nested = _design_dict_or_empty(_raw_get(candidate_budget, :exact_placement_budget, Dict{String, Any}()))
    nested_max = _raw_get(nested, :max_exact_placements, _raw_get(nested, :max_candidates, nothing))
    raw_max = _raw_get(candidate_budget, :max_exact_placements,
        nested_max === nothing ? 0 : nested_max)
    max_exact = max(0, Int(raw_max))
    return Dict(
        "max_exact_placements" => max_exact,
        "selection_policy" => String(_raw_get(nested, :selection_policy, "ranked_proxy_prefix")),
        "per_candidate_modes" => String.(collect(_raw_get(nested, :per_candidate_modes, Any["single_ro"]))),
        "requested" => max_exact,
    )
end

function _design_record_exact_failure!(card, code::AbstractString, reason::AbstractString;
                                       stage::AbstractString="exact_placement")
    push!(card["active_failures"], String(code))
    push!(card["active_failure_details"], Dict(
        "code" => String(code),
        "severity" => "warning",
        "stage" => String(stage),
        "blocks_recommendation" => true,
        "message_agent" => String(reason),
    ))
    return card
end

function _design_try_exact_linear_card(card, rec, target_ro::Real, tuning)
    kd_bounds = _design_bounds(tuning, :kd_bounds, (-3.0, 3.0))
    tol = _design_float(tuning, :exact_ro_tol, 0.05)
    verify_h = _design_float(tuning, :exact_verify_h, 0.02)
    updated = deepcopy(card)
    try
        placement = placer_place_bounded(_design_nid_to_rules(rec.nid), rec.inp, rec.out,
            Float64(target_ro); tol=tol, verify_h=verify_h, kd_bounds=kd_bounds)
        updated["placement_attempt"] = Dict(
            "attempted" => true,
            "status" => placement.feasible ? "unverified" : "infeasible",
            "endpoint" => "/api/design_screen",
            "mode" => "single_ro",
            "target_ro" => Float64(target_ro),
            "kd_bounds_used" => [kd_bounds[1], kd_bounds[2]],
            "pass" => getproperty(placement, :feasible) ? getproperty(placement, :pass) : false,
        )
        if placement.feasible && placement.pass
            cheb = Float64(placement.chebyshev_radius)
            isfinite(cheb) || (cheb = 0.0)
            metrics = updated["metrics"]
            metrics["chebyshev_radius"] = round(cheb; digits=4)
            metrics["chebyshev_radius_source"] = "placer_place_bounded"
            metrics["chebyshev_units"] = "log10_qK_euclidean"
            metrics["predicted_RO"] = round(Float64(placement.predicted_RO); digits=4)
            metrics["measured_RO"] = round(Float64(placement.measured_RO); digits=4)
            metrics["exact_vertex_idx"] = Int(placement.vertex_idx)
            metrics["exact_solve_mode"] = String(placement.solve_mode)

            constraints = updated["constraints"]
            constraints["bounds_intersection_verified"] = true
            constraints["exact_ro_tol"] = tol
            constraints["exact_verify_h"] = verify_h
            constraints["supported_handles"] = sort(collect(Set(vcat(
                String.(constraints["supported_handles"]),
                ["reaction_order_single_slope", "exact_linear_cell_chebyshev"]))))
            push!(constraints["supported_constraints"], Dict(
                "path" => "target",
                "kind" => "reaction_order_single_slope",
                "requested" => Float64(target_ro),
                "effective" => Float64(placement.predicted_RO),
                "support_level" => "enforced",
                "stage" => "exact_placement",
                "solver" => "placer_place_bounded",
                "reason" => "single exact RO target admits a linear ROP cell certificate",
            ))

            theta = updated["parameter_recommendation"]["theta_star"]
            theta["status"] = "computed"
            theta["source"] = "placer_place_bounded"
            theta["placement_status"] = "success"
            theta["verification_status"] = "verified"
            theta["source_type"] = "exact_solver"
            theta["source_endpoint"] = "/api/design_screen"
            theta["bounds_verified"] = true
            theta["solver_version"] = "placer_place_bounded/v1"
            theta["log_qK_basis"] = "log10_qK"
            theta["log_qK"] = Float64.(collect(placement.log_qK))
            theta["kd"] = Float64.(collect(placement.kd))
            theta["totals"] = _design_string_totals(placement.totals)
            theta["note"] = "Exact-linear single-RO placement: Chebyshev/interior point of a target regime intersected with the declared Kd box."

            updated["certificate_grade"] = "exact-linear"
            updated["screen_status"] = "exact_linear_pass"
            updated["pass"] = true
            updated["reason"] = "exact single-RO placement succeeded within declared Kd bounds"
            updated["placement_attempt"]["status"] = "success"
            updated["placement_attempt"]["chebyshev_radius"] = cheb
            updated["placement_attempt"]["measured_RO"] = Float64(placement.measured_RO)
            updated["certificate_stack"] = vcat(Any[
                Dict("grade" => "exact-linear",
                     "scope" => "single_ro_cell",
                     "soundness" => "exact_linear_cell",
                     "source" => "placer_place_bounded",
                     "supports_exact_placement" => true),
            ], updated["certificate_stack"])
            updated["tunability_score"] = max(Float64(updated["tunability_score"]),
                clamp(0.70 + 0.20 * min(cheb / max(kd_bounds[2] - kd_bounds[1], eps(Float64)), 1.0), 0.0, 1.0))
            return updated
        end
        reason = if placement.feasible
            "Exact-linear placement found a regime but forward verification failed: " *
            "target_RO=$(Float64(target_ro)), predicted_RO=$(placement.predicted_RO), measured_RO=$(placement.measured_RO)."
        else
            String(placement.reason)
        end
        code = placement.feasible ? "exact_placement_verification_failed" : "exact_placement_failed"
        _design_record_exact_failure!(updated, code, reason)
        updated["screen_status"] = "proxy_near_miss"
        updated["pass"] = false
        updated["reason"] = reason
        updated["placement_attempt"]["status"] = placement.feasible ? "unverified" : "infeasible"
        updated["placement_attempt"]["error_code"] = code
        updated["placement_attempt"]["reason"] = reason
        if placement.feasible
            updated["placement_attempt"]["measured_RO"] = Float64(placement.measured_RO)
            updated["placement_attempt"]["predicted_RO"] = Float64(placement.predicted_RO)
        end
        updated["parameter_recommendation"]["theta_star"]["note"] =
            "Exact-linear placement was attempted but did not produce a verified point; inspect active_failures and reason."
        return updated
    catch e
        reason = "exact placement error: $(sprint(showerror, e))"
        _design_record_exact_failure!(updated, "exact_placement_error", reason)
        updated["screen_status"] = "proxy_near_miss"
        updated["pass"] = false
        updated["reason"] = reason
        updated["placement_attempt"] = Dict(
            "attempted" => true,
            "status" => "error",
            "endpoint" => "/api/design_screen",
            "mode" => "single_ro",
            "target_ro" => Float64(target_ro),
            "kd_bounds_used" => [kd_bounds[1], kd_bounds[2]],
            "pass" => false,
            "error_code" => "exact_placement_error",
            "reason" => reason,
        )
        updated["parameter_recommendation"]["theta_star"]["note"] =
            "Exact-linear placement errored; proxy metrics remain available but this card is not a verified recommendation."
        return updated
    end
end

function _design_card_less(a, b, preferences)
    for pref in preferences
        av = _design_card_preference_value(a, pref)
        bv = _design_card_preference_value(b, pref)
        av == bv && continue
        return av > bv
    end
    return _design_card_sort_key(a) < _design_card_sort_key(b)
end

function design_screen(target_kind::AbstractString, target;
                       candidate_budget=Dict{String, Any}(),
                       tuning=Dict{String, Any}(),
                       ranking_policy=Dict{String, Any}(),
                       designability_spec=nothing)
    spec = if designability_spec isa AbstractDict &&
              String(_raw_get(designability_spec, :schema_version, "")) == DESIGNABILITY_SPEC_VERSION
        Dict{String, Any}(_materialize(designability_spec))
    else
        base = legacy_designability_spec(target_kind, target;
            candidate_budget = candidate_budget,
            tuning = tuning,
            ranking_policy = ranking_policy)
        if designability_spec isa AbstractDict
            for (key, value) in pairs(_materialize(designability_spec))
                skey = String(key)
                skey in ("target_kind", "target") && continue
                base["target"][skey] = value
            end
        end
        base
    end
    return design_screen_from_spec(spec)
end

# POST /api/design_search — { target_kind:"sign"|"exact", target:"+-+" | [1.5,0,-1] }
function handle_design_search(req)
    body = read_json(req)
    res = try
        kind, target = _design_normalize_design_target(
            _raw_get(body, :target_kind, "sign"),
            _raw_get(body, :target, nothing))
        design_search(kind, target)
    catch e
        return error_response("design_search failed: $(sprint(showerror, e))"; status = 400)
    end
    return json_response(res)
end

# POST /api/design_screen — tunability-aware wrapper around design_search.
function handle_design_screen(req)
    body = read_json(req)
    res = try
        spec = if _raw_haskey(body, :designability_spec)
            _raw_get(body, :designability_spec, nothing)
        else
            base = legacy_designability_spec(
                _raw_get(body, :target_kind, "sign"),
                _raw_get(body, :target, nothing);
                candidate_budget = _raw_get(body, :candidate_budget, Dict{String, Any}()),
                tuning = _raw_get(body, :tuning, Dict{String, Any}()),
                ranking_policy = _raw_get(body, :ranking_policy, Dict{String, Any}()),
            )
            for (key, value) in pairs(_materialize(body))
                skey = String(key)
                skey in ("target_kind", "target", "candidate_budget", "tuning", "ranking_policy") && continue
                base["target"][skey] = value
            end
            base
        end
        design_screen_from_spec(spec)
    catch e
        return error_response("design_screen failed: $(sprint(showerror, e))"; status = 400)
    end
    return json_response(res)
end

# GET/POST /api/design_labels — the behavior base-motif vocab (16 labels) with a
# representative RO program each, so the UI can show "label → RO language". Sourced
# from the tracked data products (motif_rep_prog.json + periodic_table.json
# vocab.base_motifs) so the picker never drifts from the atlas.
function handle_design_labels(req)
    dir = normpath(joinpath(@__DIR__, "..", "..", "paper_rop_periodic_table", "data"))
    rep_path   = joinpath(dir, "motif_rep_prog.json")
    vocab_path = joinpath(dir, "periodic_table.json")
    isfile(rep_path)   || return error_response("motif_rep_prog.json not found at $rep_path"; status = 500)
    isfile(vocab_path) || return error_response("periodic_table.json not found at $vocab_path"; status = 500)
    rep = JSON3.read(read(rep_path, String))            # label -> "1 → 0 → -1"
    pt  = JSON3.read(read(vocab_path, String))
    counts = Dict{String, Int}()
    vm = get(pt, :vocab, nothing)
    if vm !== nothing
        bm = get(vm, :base_motifs, nothing)
        if bm !== nothing
            for (k, v) in pairs(bm); counts[String(k)] = Int(v); end
        end
    end
    labels = [Dict("label" => String(k),
                    "ro_program" => String(rep[k]),
                    "count" => get(counts, String(k), 0))
              for k in sort(collect(keys(rep)))]
    return json_response(Dict("labels" => labels))
end

# Best-effort NUMERIC level solve (the quantitative layer, NOT a clean regime solve):
# adjust one total so the output reaches a target level at a given operating input.
function placer_level(rules::Vector{String}, kd::Vector{Float64}, totals::Dict{Symbol, Float64},
                      input_sym, output_sym, operating_input::Real, target_level::Real, adjust_sym)
    input_sym = Symbol(input_sym); output_sym = Symbol(output_sym); adjust_sym = Symbol(adjust_sym)
    model, _, _, _ = build_model(rules, kd)
    input_idx  = locate_sym_qK(model, input_sym);  input_idx  === nothing && error("unknown input $input_sym")
    adjust_idx = locate_sym_qK(model, adjust_sym); adjust_idx === nothing && error("unknown adjust total $adjust_sym")
    output_idx = locate_sym_x(model, output_sym);  output_idx === nothing && error("unknown output $output_sym")
    qsyms = Symbol.(string.(q_sym(model)))
    base = zeros(Float64, model.n)
    for i in 1:model.d; base[i] = log10(get(totals, qsyms[i], 1.0)); end
    for j in 1:model.r; base[model.d + j] = log10(kd[j]); end
    base[input_idx] = log10(Float64(operating_input))
    out_at(la) = begin
        p = copy(base); p[adjust_idx] = la
        qK2x(model, p; input_logspace = true, output_logspace = true)[output_idx]   # log10[output]
    end
    tgt = log10(max(Float64(target_level), 1e-300))
    lo, hi = -4.0, 8.0
    flo = out_at(lo) - tgt; fhi = out_at(hi) - tgt
    local la::Float64; feasible = true
    if flo * fhi > 0
        la = abs(flo) < abs(fhi) ? lo : hi; feasible = false
    else
        for _ in 1:60
            mid = (lo + hi) / 2; fm = out_at(mid) - tgt
            if flo * fm <= 0; hi = mid; else; lo = mid; flo = fm; end
        end
        la = (lo + hi) / 2
    end
    nt = copy(totals); nt[adjust_sym] = exp10(la)
    return (; feasible, adjusted_total = adjust_sym, adjusted_value = exp10(la),
            target_level = Float64(target_level), achieved_level = exp10(out_at(la)),
            kd = collect(Float64.(kd)), totals = nt)
end
# POST /api/placer_level — { rules|session, input_sym, output_sym, kd, totals, operating_input, target_level, adjust_sym }
function handle_placer_level(req)
    body = read_json(req)
    rules = if _raw_haskey(body, :rules)
        String.(collect(_raw_get(body, :rules, String[])))
    else
        bundle, err = _resolve_bundle_or_response(body); err === nothing || return err
        String.(collect(bundle["rules"]))
    end
    isempty(rules) && return error_response("No reactions"; status = 400)
    for k in (:input_sym, :output_sym, :operating_input, :target_level, :adjust_sym, :kd)
        _raw_haskey(body, k) || return error_response("`$k` is required"; status = 400)
    end
    input_sym  = Symbol(_raw_get(body, :input_sym, ""))
    output_sym = Symbol(_raw_get(body, :output_sym, ""))
    kd = Float64.(collect(_raw_get(body, :kd, Float64[])))
    totals = Dict{Symbol, Float64}()
    tw = _raw_get(body, :totals, nothing)
    tw === nothing || for (k, v) in pairs(tw); totals[Symbol(k)] = Float64(v); end
    res = try
        placer_level(rules, kd, totals, input_sym, output_sym,
                     Float64(_raw_get(body, :operating_input, 1.0)),
                     Float64(_raw_get(body, :target_level, 1.0)),
                     Symbol(_raw_get(body, :adjust_sym, "")))
    catch e
        return error_response("Level solve failed: $(sprint(showerror, e))"; status = 400)
    end
    kd_vec = collect(Float64.(res.kd))
    curve = try
        placer_dose_response(rules, kd_vec, res.totals, input_sym, String(output_sym))
    catch e
        return error_response("Dose-response curve failed: $(sprint(showerror, e))"; status = 400)
    end
    return json_response(Dict(
        "adjusted_total" => string(res.adjusted_total), "adjusted_value" => res.adjusted_value,
        "target_level" => res.target_level, "achieved_level" => json_safe_real(res.achieved_level),
        "feasible" => res.feasible, "totals" => Dict(string(k) => v for (k, v) in res.totals),
        "dose_response_curve" => curve,
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

include(joinpath(@__DIR__, "designability_feasible_regions.jl"))
include(joinpath(@__DIR__, "designability.jl"))
include(joinpath(@__DIR__, "routing.jl"))

end # module
