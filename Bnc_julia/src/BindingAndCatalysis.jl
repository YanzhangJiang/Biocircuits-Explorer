__precompile__(true)
module BindingAndCatalysis

include(joinpath(@__DIR__, "initialize.jl"))

export Bnc
export update_catalysis!

# include("helperfunctions.jl")
export locate_sym_x, locate_sym_qK, pythonprint, N_generator, L_generator, randomize
export parse_linear_combination

# include("qK_x_mapping.jl")
export x2qK, qK2x, x_traj_with_qK_change, x_traj_with_q_change, x_traj_cat, qK_traj_cat 

# include("volume_calc.jl")
export calc_volume

# include("numeric.jl")
export logder_x_qK, logder_qK_x, ∂logx_∂logqK, ∂logqK_∂logx
export scan_parameter_1d, scan_parameter_2d

# include("regime_enumerate.jl")
export find_all_vertices

# include("regimes.jl")
export find_all_vertices!, get_vertices_perm_dict, get_nullities, get_volumes, have_perm
export get_vertices
export get_vertices_neighbor_mat
export is_singular, is_asymptotic, n_vertices

export get_idx, get_perm, get_vertex, get_neighbors, get_nullity, get_one_inner_point
export get_P_P0, get_P,get_P0
export get_M_M0, get_M, get_M0
export get_H_H0,get_H,get_H0
export get_C_C0_x,get_C_x, get_C0_x
export get_C_C0_nullity_qK, get_C_C0_qK, get_C_qK, get_C0_qK
export get_C_C0_nullity, get_C_C0, get_C, get_C0

export check_feasibility_with_constraint, feasible_vertieces_with_constraint
export get_polyhedron, get_volume
export is_neighbor, get_interface, get_change_dir
export get_function


# include("regime_assign.jl")
export assign_vertex, assign_vertex_qK, assign_vertex_x
# include("symbolics.jl")
export x_sym, q_sym, K_sym, qK_sym, ∂logqK_∂logx_sym, ∂logx_∂logqK_sym, logder_qK_x_sym, logder_x_qK_sym
export show_condition_poly, show_condition_x, show_condition_qK, show_condition
export show_expression_mapping, show_expression_x, show_expression_qK, show_expression_path
export show_dominant_condition, show_conservation, show_equilibrium, show_interface
export sym_direction, print_path, print_paths, format_arrow

# include("regime_graphs.jl")
export get_vertices_graph!, SISOPaths,  get_polyhedra, get_polyhedron, get_SISO_graph
export get_path, get_edge, get_intersect
export get_neighbor_graph_x, get_neighbor_graph_qK,get_neighbor_graph
export get_sources, get_sinks, get_sources_sinks
export get_RO_path, group_sum, get_RO_paths, summary_RO_path
export get_behavior_families, summary_behavior_families
export get_vertex_rop_coords, get_edge_rop_segment, compute_rop_polyhedron
# export get_volume, get_C_C0_nullity_qK

# include("visualize.jl")
export SISO_plot, get_edge_labels,set_proper_bounds_for_graph_plot!
export get_node_positions, get_node_colors, get_node_labels, get_node_size
export draw_graph, add_vertices_idx!,add_arrows!,add_nodes_text!, set_node_positions
export draw_qK_neighbor_grh, find_bounds, add_rgm_colorbar!, get_color_map
export get_ROP_plot_data, draw_ROP

end # module
