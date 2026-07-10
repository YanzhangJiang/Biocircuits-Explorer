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
