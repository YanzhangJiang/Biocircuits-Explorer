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
