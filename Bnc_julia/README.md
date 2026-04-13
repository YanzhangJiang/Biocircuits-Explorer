# BindingAndCatalysis.jl

BindingAndCatalysis.jl models equilibrium binding networks and catalysis-driven dynamics. It provides tools to:

- construct binding networks from stoichiometry or conservation laws
- enumerate regimes (vertices) defined by dominant species
- map between concentration space (`x`) and total/binding-constant space (`qK`)
- analyze regime graphs and SISO (single-input/single-output) paths
- visualize regimes, regime interfaces, and trajectories

## Installation

For local development:

```julia
using Pkg
Pkg.develop(path="/path/to/BindingAndCatalysis.jl")
Pkg.instantiate()
```

To work with the examples, activate the Examples environment:

```julia
using Pkg
Pkg.activate("Examples")
Pkg.instantiate()
```

## Quick start

Below is a minimal binding network for monomer-dimer binding:

```julia
using BindingAndCatalysis
using CairoMakie

model = let
    N = [1 1 -1]
    x_sym = [:E, :S, :C]
    q_sym = [:tE, :tS]
    K_sym = [:K]
    Bnc(N = N, x_sym = x_sym, q_sym = q_sym, K_sym = K_sym)
end

show_conservation(model)           # q = Lx
show_equilibrium(model)            # K and x equilibrium expressions
summary(model)
```

### Regime enumeration and queries

```julia
find_all_vertices!(model)
get_vertices_perm_dict(model)

vtx = get_vertex(model, 1)
show_condition_x(vtx)
show_condition_qK(vtx)

get_P_P0(vtx)
get_H_H0(vtx)
get_C_C0_nullity(vtx)
```

### Regime graphs and SISO paths

```julia
vtx_grh = get_vertices_graph!(model, full=true)

# qK neighbor graph
qk_graph = get_neighbor_graph_qK(model)

# single-input/single-output paths
paths = SISOPaths(model, :tS)
summary(paths; show_volume=false)
show_expression_path(paths, 1; log_space=false)
```

### Mapping between qK and x

```julia
logqK = randomize(model, 1; log_lower=-6, log_upper=6)[1]
logx = qK2x(model, logqK; input_logspace=true, output_logspace=true)
logqK_back = x2qK(model, logx; input_logspace=true, output_logspace=true)
```

## Notebook walkthrough

The best end-to-end tutorial is in [`Examples/Minimal_example.ipynb`](Examples/Minimal_example.ipynb). It walks through:

- model construction and symbol helpers
- regime enumeration and classification
- regime graph visualization and interfaces
- numerical mapping between `qK` and `x`
- more advanced SISO path analysis

To run the notebook, make sure Jupyter is configured with Julia via IJulia.jl. Follow the official IJulia documentation to install and register the Julia kernel: <https://julialang.github.io/IJulia.jl/stable/>. When installing Julia for the first time, you may want to add a multi-threaded Jupyter kernel so the notebook can take advantage of multiple CPU threads. For example:

```julia
installkernel(
    "Julia (multi threads)",
    env = Dict(
        "JULIA_NUM_THREADS" => "auto",
        # Enable this if you are working on a remote machine (e.g. via SSH)
        # and want changes in your package source code to take effect
        # immediately in Jupyter via Revise.jl.
        # "JULIA_REVISE_POLL" => "1"
    )
)
```

This creates a Jupyter kernel named “Julia (multi threads)” with automatic multi-thread configuration.

## API overview

Core entry points:

- `Bnc(; N, L, x_sym, q_sym, K_sym, S, aT, k, cat_x_idx)` for model creation
- `find_all_vertices!(model)` to enumerate regimes
- `get_vertex(model, idx_or_perm)` for regime objects
- `get_vertices_graph!(model)` and `get_neighbor_graph_qK(model)` for graphs
- `qK2x` / `x2qK` for numerical mapping
- `show_condition_*` and `show_expression_*` for symbolic inspection

See the inline docstrings in `src/` for full details.

## License

See [`LICENSE`](LICENSE).
