# tools/migration_parity/capture_goldens.jl
#
# Run this script NON-HEADLESS so that visualize.jl is loaded and get_ROP_plot_data is defined:
#
#   BNC_HEADLESS=0 julia --compiled-modules=no --project=webapp \
#       tools/migration_parity/capture_goldens.jl
#
# The goldens are written to PARITY_OUT (defaults to tools/migration_parity/goldens/).
# Each fixture produces <name>.json; a _index.json lists all fixture names.
#
# This is CAPTURE-CURRENT-AS-TRUTH: we record whatever the engine produces.
# Do not hard-code expected values.

using BindingAndCatalysis
import JSON3

# Confirm we are running non-headless so get_ROP_plot_data is available.
if BindingAndCatalysis._BNC_HEADLESS
    @warn """
    _BNC_HEADLESS=true — visualize.jl was skipped, get_ROP_plot_data is UNDEFINED.
    Re-run with:
        BNC_HEADLESS=0 julia --compiled-modules=no --project=webapp tools/migration_parity/capture_goldens.jl
    """
    error("Must run with BNC_HEADLESS=0 and --compiled-modules=no")
end

# Suppress Bnc progress meters.
ENV["BNC_NO_PROGRESS"] = "1"

# Output directory.
const PARITY_OUT = get(ENV, "PARITY_OUT", joinpath(@__DIR__, "goldens"))
mkpath(PARITY_OUT)

# Load fixture definitions.
include(joinpath(@__DIR__, "fixtures.jl"))

# ── JSON3 serialization helpers ──────────────────────────────────────────────

"""Recursively convert a NamedTuple / Vector / Matrix to plain Dict/Array for JSON3.
NaN and Inf values are converted to null (JSON3 does not allow them per spec)."""
function to_json_val(x)
    if x isa NamedTuple
        Dict(String(k) => to_json_val(v) for (k, v) in pairs(x))
    elseif x isa AbstractDict
        Dict(String(k) => to_json_val(v) for (k, v) in x)
    elseif x isa AbstractMatrix
        [to_json_val(row) for row in eachrow(x)]   # matrix → row-vectors
    elseif x isa AbstractVector
        [to_json_val(el) for el in x]
    elseif x isa Tuple
        [to_json_val(el) for el in x]
    elseif x isa Symbol
        String(x)
    elseif x isa AbstractFloat && !isfinite(x)
        # JSON spec forbids NaN and Inf; serialize as null so goldens are valid JSON.
        # The parity check must handle null vs NaN equivalence.
        nothing
    else
        x  # Float64, Int, Bool, String, nothing
    end
end

# ── Main capture loop ─────────────────────────────────────────────────────────

fixture_list = fixtures()
captured_names = String[]

for fx in fixture_list
    name        = fx.name
    model       = fx.model
    pairs_v     = fx.pairs   # Vector{Tuple{Symbol,Symbol}}
    output_expr = fx.output_expr
    param_idx   = fx.param_idx
    param_range = fx.param_range

    println("=" ^ 60)
    println("Fixture: $name")

    # ── 1. Enumerate vertices ─────────────────────────────────────────────────
    find_all_vertices!(model)
    nv = n_vertices(model)
    println("  n_vertices = $nv")

    perms = [collect(get_perm(model, i)) for i in 1:nv]

    # ── 2. Behavior families (requires SISOPaths) ─────────────────────────────
    # Identify the qK symbol at param_idx to build the SISOPaths.
    qK_syms = qK_sym(model)    # Vector{Num} = [q_syms...; K_syms...]
    change_sym = Symbol(string(qK_syms[param_idx]))
    println("  change_qK_sym = $change_sym")

    siso = SISOPaths(model, change_sym)
    bf = get_behavior_families(
        siso;
        observe_x  = Symbol(output_expr),
        path_scope = :feasible,
    )

    # Serialize the behavior_families result.
    # bf is a NamedTuple; use to_json_val to flatten it.
    bf_json = to_json_val(bf)

    # ── 3. scan_parameter_1d ──────────────────────────────────────────────────
    # fixed_params = log10(all-ones qK) with param_idx removed.
    # The all-ones vector (log10 scale → zeros) is a canonical neutral point.
    n_qK = model.d + model.r
    full_logqK = zeros(Float64, n_qK)          # log10(1) = 0 for every parameter
    fixed_params = deleteat!(copy(full_logqK), param_idx)

    # parse output coefficients
    output_coeffs = parse_linear_combination(model, output_expr)

    param_vals, output_traj, regimes, valid = scan_parameter_1d(
        model, param_idx, param_range, [output_coeffs], fixed_params;
        input_logspace  = true,
        output_logspace = true,
        track_validity  = true,
    )
    println("  scan_1d: n_pts=$(length(param_vals)), n_valid=$(count(valid))")

    scan_1d_json = Dict(
        "param_vals" => collect(param_vals),
        "out"        => [collect(output_traj[:, 1])],   # 1 output expression
        "regime"     => collect(Int, regimes),
        "valid"      => collect(valid),
    )

    # ── 4. get_ROP_plot_data ──────────────────────────────────────────────────
    # pairs_v is already Vector{Tuple{Symbol,Symbol}} — matches the API signature.
    println("  get_ROP_plot_data with pairs = $pairs_v")
    rop_data = get_ROP_plot_data(model, pairs_v; npoints = 5000)
    rop_json = to_json_val(rop_data)

    # ── 5. Write golden JSON ──────────────────────────────────────────────────
    golden = Dict(
        "name"             => name,
        "n_vertices"       => nv,
        "perms"            => perms,
        "behavior_families"=> bf_json,
        "scan_1d"          => scan_1d_json,
        "rop_plot_data"    => rop_json,
    )

    out_path = joinpath(PARITY_OUT, "$name.json")
    open(out_path, "w") do io
        JSON3.write(io, golden)
    end
    println("  Written: $out_path")
    push!(captured_names, name)
end

# ── Write index ───────────────────────────────────────────────────────────────
index_path = joinpath(PARITY_OUT, "_index.json")
open(index_path, "w") do io
    JSON3.write(io, Dict("fixtures" => captured_names))
end
println("=" ^ 60)
println("Index written: $index_path")
println("Done. Captured $(length(captured_names)) fixtures: $(join(captured_names, ", "))")
