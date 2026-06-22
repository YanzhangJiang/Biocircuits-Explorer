# tools/migration_parity/assert_parity.jl
#
# Parity assertion harness: recompute every fixture on the current engine
# and compare to the goldens captured by capture_goldens.jl.
#
# Run via:
#   PARITY_OUT=tools/migration_parity/goldens BNC_HEADLESS=0 julia --project=webapp \
#       tools/migration_parity/assert_parity.jl
#
# Exits 0 if ALL fixtures pass, 1 otherwise.

using BindingAndCatalysis
import JSON3

ENV["BNC_NO_PROGRESS"] = "1"

# get_ROP_plot_data is now in rop/rop_plot.jl (included unconditionally), so no
# eval-hack is needed here.

const PARITY_OUT = get(ENV, "PARITY_OUT", joinpath(@__DIR__, "goldens"))

include(joinpath(@__DIR__, "fixtures.jl"))

# ── Comparison helpers ────────────────────────────────────────────────────────

"""
Walk the freshly-recomputed Julia value `fresh` against the parsed JSON golden `golden`.
`path` tracks the breadcrumb for error messages.
Returns `nothing` on success, or a String describing the first divergence.
"""
const _SKIP_STOCHASTIC_KEYS = Set(["inner_points"])

function compare(fresh, golden, path::String = ""; tol::Float64 = 1e-9)::Union{Nothing, String}

    # ── NaN / Inf  ↔  JSON null ───────────────────────────────────────────────
    if fresh isa AbstractFloat && !isfinite(fresh)
        if golden === nothing
            return nothing   # EQUAL: non-finite ↔ null
        else
            return "$path: recomputed is $(fresh) but golden is $golden (expected null for non-finite)"
        end
    end
    if golden === nothing
        if fresh === nothing
            return nothing
        end
        # If golden is null but fresh is a finite float, treat as divergence.
        return "$path: recomputed is $fresh but golden is null"
    end

    # ── nothing ↔ null ───────────────────────────────────────────────────────
    if fresh === nothing && golden === nothing
        return nothing
    end
    if fresh === nothing
        return "$path: recomputed is nothing but golden is $golden"
    end

    # ── NamedTuple ────────────────────────────────────────────────────────────
    if fresh isa NamedTuple
        # Convert NamedTuple to Dict-like; golden should be a Dict (from JSON3)
        if !(golden isa AbstractDict)
            return "$path: type mismatch — recomputed NamedTuple vs golden $(typeof(golden))"
        end
        fresh_keys = Set(String(k) for k in keys(fresh))
        golden_keys = Set(string(k) for k in keys(golden))
        if fresh_keys != golden_keys
            extra = setdiff(fresh_keys, golden_keys)
            missing_ = setdiff(golden_keys, fresh_keys)
            return "$path: key mismatch — extra=$extra missing=$missing_"
        end
        for k in keys(fresh)
            ks = String(k)
            ks in _SKIP_STOCHASTIC_KEYS && continue   # skip stochastic sampled fields
            sub = compare(fresh[k], golden[ks], isempty(path) ? ks : "$path.$ks"; tol)
            sub === nothing || return sub
        end
        return nothing
    end

    # ── AbstractDict ─────────────────────────────────────────────────────────
    if fresh isa AbstractDict
        if !(golden isa AbstractDict)
            return "$path: type mismatch — recomputed Dict vs golden $(typeof(golden))"
        end
        fresh_keys = Set(String(k) for k in keys(fresh))
        golden_keys = Set(string(k) for k in keys(golden))
        if fresh_keys != golden_keys
            extra = setdiff(fresh_keys, golden_keys)
            missing_ = setdiff(golden_keys, fresh_keys)
            return "$path: key mismatch — extra=$extra missing=$missing_"
        end
        for k in keys(fresh)
            ks = String(k)
            ks in _SKIP_STOCHASTIC_KEYS && continue   # skip stochastic sampled fields
            sub = compare(fresh[k], golden[ks], isempty(path) ? ks : "$path.$ks"; tol)
            sub === nothing || return sub
        end
        return nothing
    end

    # ── AbstractMatrix ────────────────────────────────────────────────────────
    if fresh isa AbstractMatrix
        # capture_goldens serializes matrices as row-vectors (array of arrays)
        if !(golden isa AbstractVector)
            return "$path: type mismatch — recomputed Matrix vs golden $(typeof(golden))"
        end
        if size(fresh, 1) != length(golden)
            return "$path: matrix row count mismatch — recomputed $(size(fresh,1)) vs golden $(length(golden))"
        end
        for i in 1:size(fresh, 1)
            sub = compare(collect(fresh[i, :]), golden[i], "$path[$i]"; tol)
            sub === nothing || return sub
        end
        return nothing
    end

    # ── AbstractVector / Tuple ────────────────────────────────────────────────
    if fresh isa Union{AbstractVector, Tuple}
        if !(golden isa AbstractVector)
            return "$path: type mismatch — recomputed $(typeof(fresh)) vs golden $(typeof(golden))"
        end
        if length(fresh) != length(golden)
            return "$path: length mismatch — recomputed $(length(fresh)) vs golden $(length(golden))"
        end
        for i in eachindex(fresh)
            sub = compare(fresh[i], golden[i], "$path[$i]"; tol)
            sub === nothing || return sub
        end
        return nothing
    end

    # ── Symbol → String exact ─────────────────────────────────────────────────
    if fresh isa Symbol
        gs = golden isa AbstractString ? golden : string(golden)
        if String(fresh) != gs
            return "$path: Symbol mismatch — recomputed :$fresh vs golden \"$gs\""
        end
        return nothing
    end

    # ── Float ↔ Float (finite) ───────────────────────────────────────────────
    if fresh isa AbstractFloat
        if !(golden isa Number)
            return "$path: type mismatch — recomputed Float $(fresh) vs golden $(typeof(golden))"
        end
        gf = Float64(golden)
        if abs(fresh - gf) > tol
            return "$path: float divergence — |$(fresh) - $(gf)| = $(abs(fresh-gf)) > tol=$tol"
        end
        return nothing
    end

    # ── Integer ──────────────────────────────────────────────────────────────
    if fresh isa Integer
        if !(golden isa Number)
            return "$path: type mismatch — recomputed Int $(fresh) vs golden $(typeof(golden))"
        end
        if Int64(fresh) != Int64(round(golden))
            return "$path: int mismatch — recomputed $fresh vs golden $golden"
        end
        return nothing
    end

    # ── Bool ─────────────────────────────────────────────────────────────────
    if fresh isa Bool
        gb = golden isa Bool ? golden : Bool(golden)
        if fresh != gb
            return "$path: bool mismatch — recomputed $fresh vs golden $gb"
        end
        return nothing
    end

    # ── String ───────────────────────────────────────────────────────────────
    if fresh isa AbstractString
        gs = golden isa AbstractString ? golden : string(golden)
        if fresh != gs
            return "$path: string mismatch — recomputed \"$fresh\" vs golden \"$gs\""
        end
        return nothing
    end

    # ── Struct with fieldnames (e.g. Volume{mean,var}) ────────────────────────
    # The engine returns typed structs (e.g. Volume) that to_json_val serializes as Dicts.
    # Treat any non-primitive struct as a dict-like by walking its fieldnames.
    if !isempty(fieldnames(typeof(fresh)))
        if !(golden isa AbstractDict)
            return "$path: type mismatch — recomputed struct $(typeof(fresh)) vs golden $(typeof(golden))"
        end
        fresh_keys = Set(String(f) for f in fieldnames(typeof(fresh)))
        golden_keys = Set(string(k) for k in keys(golden))
        if fresh_keys != golden_keys
            extra = setdiff(fresh_keys, golden_keys)
            missing_ = setdiff(golden_keys, fresh_keys)
            return "$path: struct field/key mismatch — extra=$extra missing=$missing_"
        end
        for f in fieldnames(typeof(fresh))
            sub = compare(getfield(fresh, f), golden[String(f)],
                          isempty(path) ? String(f) : "$path.$(String(f))"; tol)
            sub === nothing || return sub
        end
        return nothing
    end

    # ── Fallback ─────────────────────────────────────────────────────────────
    if fresh != golden
        return "$path: mismatch — recomputed $(repr(fresh)) vs golden $(repr(golden))"
    end
    return nothing
end

# ── Helper: load golden JSON as plain Julia Dict (not JSON3 types) ─────────────

function load_golden(name::String)
    path = joinpath(PARITY_OUT, "$name.json")
    isfile(path) || error("Golden not found: $path")
    # JSON3.read returns typed JSON3 objects; convert to plain Julia via JSON3.write -> JSON3.read(Any)
    raw = read(path, String)
    return JSON3.read(raw, Any)
end

# ── Helper: deep-convert JSON3 types to plain Julia (Dict / Vector / primitives) ─

function jsonval(x)
    if x isa JSON3.Object
        d = Dict{String,Any}()
        for (k, v) in x
            d[String(k)] = jsonval(v)
        end
        return d
    elseif x isa JSON3.Array
        return [jsonval(el) for el in x]
    else
        return x   # Bool, Int, Float64, String, Nothing
    end
end

# ── Main assertion loop ───────────────────────────────────────────────────────

fixture_list = fixtures()
all_pass = true

for fx in fixture_list
    name        = fx.name
    model       = fx.model
    pairs_v     = fx.pairs
    output_expr = fx.output_expr
    param_idx   = fx.param_idx
    param_range = fx.param_range

    println("Checking: $name")

    # Load the golden.
    golden = jsonval(load_golden(name))

    # ── Recompute (byte-identical calls to capture_goldens.jl) ───────────────

    # 1. n_vertices + perms
    find_all_vertices!(model)
    nv_fresh   = n_vertices(model)
    perms_fresh = [collect(get_perm(model, i)) for i in 1:nv_fresh]

    # 2. behavior_families
    qK_syms    = qK_sym(model)
    change_sym = Symbol(string(qK_syms[param_idx]))
    siso       = SISOPaths(model, change_sym)
    bf_fresh   = get_behavior_families(
        siso;
        observe_x  = Symbol(output_expr),
        path_scope = :feasible,
    )

    # 3. scan_parameter_1d
    n_qK        = model.d + model.r
    full_logqK  = zeros(Float64, n_qK)
    fixed_params = deleteat!(copy(full_logqK), param_idx)
    output_coeffs = parse_linear_combination(model, output_expr)

    param_vals_fresh, output_traj_fresh, regimes_fresh, valid_fresh = scan_parameter_1d(
        model, param_idx, param_range, [output_coeffs], fixed_params;
        input_logspace  = true,
        output_logspace = true,
        track_validity  = true,
    )
    scan_1d_fresh = Dict(
        "param_vals" => collect(param_vals_fresh),
        "out"        => [collect(output_traj_fresh[:, 1])],
        "regime"     => collect(Int, regimes_fresh),
        "valid"      => collect(valid_fresh),
    )

    # 4. get_ROP_plot_data
    rop_fresh = get_ROP_plot_data(model, pairs_v; npoints = 5000)

    # ── Build a fresh composite matching the golden top-level structure ───────
    fresh = Dict(
        "name"              => name,
        "n_vertices"        => nv_fresh,
        "perms"             => perms_fresh,
        "behavior_families" => bf_fresh,
        "scan_1d"           => scan_1d_fresh,
        "rop_plot_data"     => rop_fresh,
    )

    # ── Compare ──────────────────────────────────────────────────────────────
    err = compare(fresh, golden)
    if err === nothing
        println("PASS $name")
    else
        println("FAIL $name: $err")
        global all_pass = false
    end
end

println()
all_pass ? println("All fixtures PASS.") : println("Some fixtures FAILED.")
exit(all_pass ? 0 : 1)
