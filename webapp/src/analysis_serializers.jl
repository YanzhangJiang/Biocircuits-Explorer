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
