# === ROP periodic-table layer (LOCAL OVERLAY): plot-data extractor ===============
# Makie-free; included unconditionally so the web /api ROP endpoint works headless.

"""
    get_ROP_plot_data(model::Bnc, pairs::AbstractVector{<:Tuple{Any, Any}};
        add_inner_points=true, npoints=50000, singular_extends=2.0)
    -> NamedTuple

Build geometry data for 2D/3D ROP-space visualization.

Each element of `pairs` is `(x_symbol, qK_symbol)` and defines one axis:
`∂log(x)/∂log(qK)`.
"""
function get_ROP_plot_data(model::Bnc, pairs::AbstractVector{<:Tuple{Any, Any}};
    add_inner_points::Bool=true,
    npoints::Int=50000,
    singular_extends::Float64=2.0)

    V = get_vertices(model, singular=1, return_idx=true)

    V_non_singular = filter(V) do v
        !is_singular(model, v)
    end

    V_singular = filter(V) do v
        is_singular(model, v)
    end

    neighbor_mat = get_vertices_neighbor_mat(model)
    singular_neighbor_mat = neighbor_mat[V_singular, V_singular]
    nonsingular_neighbor_mat = neighbor_mat[V_non_singular, V_non_singular]

    vtx_bag = [(Set{Int}(), Set{Int}()) for _ in eachindex(V_non_singular)]

    rgm_dct = let
        groups, labels = connected_components_sparse(singular_neighbor_mat)
        dct = Dict{Int, Set{Int}}()
        for i in eachindex(V_singular)
            dct[i] = Set(groups[labels[i]])
        end
        dct
    end

    function get_direct_neighbor_with_singular_regime(i)
        nbs = Int[]
        for (idx, j) in enumerate(V_non_singular)
            if neighbor_mat[i, j] == 1
                push!(nbs, idx)
            end
        end
        nbs
    end

    function fill_indirect_adj!(j)
        rgms = getindex.(Ref(rgm_dct), collect(vtx_bag[j][1]))
        all_rgms = isempty(rgms) ? Set{Int}() : union(rgms...)
        union!(vtx_bag[j][2], setdiff(all_rgms, vtx_bag[j][1]))
    end

    for (idx, i) in enumerate(V_singular)
        nbs = get_direct_neighbor_with_singular_regime(i)
        for nb in nbs
            push!(vtx_bag[nb][1], idx)
        end
    end

    for j in eachindex(vtx_bag)
        fill_indirect_adj!(j)
    end

    direct_neighbor_pairs = let
        I, J, _ = findnz(tril(nonsingular_neighbor_mat))
        collect(zip(I, J))
    end

    new_form_neighbor_mat = let
        neighbor_mat_compressed = compress_adjacency(neighbor_mat, V_non_singular)
        dropzeros!(neighbor_mat_compressed .- nonsingular_neighbor_mat)
    end

    indirect_neighbor_pairs = let
        I, J, _ = findnz(tril(new_form_neighbor_mat))
        collect(zip(I, J))
    end

    if length(pairs) > 3
        @warn "More than 3 pairs provided, only the first 3 will be used for 3D visualization."
        pairs = pairs[1:3]
    end
    if length(pairs) < 2
        error("At least 2 pairs are needed for visualization.")
    end

    pair_indices = pairs .|> p -> (locate_sym_x(model, p[1]), locate_sym_qK(model, p[2]))
    axis_labels = pair_indices .|> p -> "∂log $(string(x_sym(model)[p[1]]))/∂log $(string(qK_sym(model)[p[2]]))"
    pair_labels = pair_indices .|> p -> (
        x_symbol = string(x_sym(model)[p[1]]),
        qK_symbol = string(qK_sym(model)[p[2]]),
        label = "∂log $(string(x_sym(model)[p[1]]))/∂log $(string(qK_sym(model)[p[2]]))",
    )

    get_val(H) = [Float64(H[pair...]) for pair in pair_indices]
    get_col(i) = is_asymptotic(model, i) ? :asymptotic : :regular

    point_coords = get_H.(Ref(model), V_non_singular) .|> get_val
    singular_dirs = get_H.(Ref(model), V_singular) .|> get_val

    point_records = map(eachindex(V_non_singular)) do i
        (
            coords = point_coords[i],
            vertex_idx = V_non_singular[i],
            perm = collect(get_perm(model, V_non_singular[i])),
            point_type = get_col(V_non_singular[i]),
        )
    end

    direct_edges = map(direct_neighbor_pairs) do (i, j)
        (
            from = point_coords[i],
            to = point_coords[j],
            from_idx = V_non_singular[i],
            to_idx = V_non_singular[j],
        )
    end

    indirect_edges = map(indirect_neighbor_pairs) do (i, j)
        (
            from = point_coords[i],
            to = point_coords[j],
            from_idx = V_non_singular[i],
            to_idx = V_non_singular[j],
        )
    end

    direct_rays = let
        rays = NamedTuple[]
        for i in eachindex(vtx_bag)
            for j in vtx_bag[i][1]
                push!(rays, (
                    from = point_coords[i],
                    to = point_coords[i] .+ singular_dirs[j] .* singular_extends,
                    from_idx = V_non_singular[i],
                    singular_idx = V_singular[j],
                ))
            end
        end
        rays
    end

    indirect_rays = let
        rays = NamedTuple[]
        for i in eachindex(vtx_bag)
            for j in vtx_bag[i][2]
                push!(rays, (
                    from = point_coords[i],
                    to = point_coords[i] .+ singular_dirs[j] .* singular_extends,
                    from_idx = V_non_singular[i],
                    singular_idx = V_singular[j],
                ))
            end
        end
        rays
    end

    inner_points = if add_inner_points
        x_smp = randomize(model, npoints)
        x_smp .|> x -> ∂logx_∂logqK(model; x=x, input_logspace=true) |> get_val
    else
        Vector{Vector{Float64}}()
    end

    return (
        dimension = length(pair_indices),
        pairs = pair_labels,
        axis_labels = axis_labels,
        points = point_records,
        direct_edges = direct_edges,
        indirect_edges = indirect_edges,
        direct_rays = direct_rays,
        indirect_rays = indirect_rays,
        inner_points = inner_points,
    )
end
