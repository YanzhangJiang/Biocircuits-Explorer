# tools/migration_parity/fixtures.jl
#
# Build the canonical fixture list for the parity test harness.
# Each fixture is a NamedTuple with fields:
#   name        :: String
#   model       :: Bnc
#   pairs       :: Vector{Tuple{Symbol,Symbol}}   (for get_ROP_plot_data)
#   output_expr :: String                          (parseable by parse_linear_combination)
#   param_idx   :: Int                             (qK index to sweep)
#   param_range :: Vector{Float64}                 (log10 range)
#
# Networks are lifted verbatim from Bnc_julia/test/runtests.jl (5 snapshot networks)
# and use the same inline build_model helper that file uses.  No webapp dependency.
#
# Source lines (runtests.jl):
#   1. single:       line 134  ["L + A <-> AL"]
#   2. competitive:  line 239  ["L + A <-> AL", "L + B <-> BL"]
#   3. cooperative:  line 279  ["A + L <-> AL", "AL + L <-> AL2"]
#   4. prozone/hook: line 309  ["L + A <-> AL", "L + B <-> BL", "AL + B <-> ALB"]
#   5. ternary:      line 341  ["A + L <-> AL", "AL + B <-> ALB"]

using BindingAndCatalysis

# ── Inline build_model replica (mirrors Bnc_julia/test/runtests.jl exactly) ──

const _ARROW_RE = r"<->|<=>|↔"

function _parse_term(term::AbstractString)
    t = strip(term)
    m = match(r"^([0-9]+)?\s*([A-Za-z_][A-Za-z0-9_]*)$", t)
    m === nothing && error("Bad term: $term")
    coeff = m.captures[1] === nothing ? 1 : parse(Int, m.captures[1])
    return Symbol(m.captures[2]), coeff
end

function _parse_side(side::AbstractString)
    dict = Dict{Symbol,Int}()
    for p in split(side, "+")
        sym, coeff = _parse_term(p)
        dict[sym] = get(dict, sym, 0) + coeff
    end
    return dict
end

function _build_N(rules::Vector{String})
    reactants = Dict{Symbol,Int}[]
    products  = Dict{Symbol,Int}[]
    for rule in rules
        m = match(_ARROW_RE, rule)
        m === nothing && error("Reaction must contain an arrow: $rule")
        left, right = split(rule, m.match)
        push!(reactants, _parse_side(left))
        push!(products, _parse_side(right))
    end
    r = length(rules)

    all_species = Set{Symbol}()
    for rd in reactants; union!(all_species, keys(rd)); end
    for pd in products;  union!(all_species, keys(pd)); end

    prod_set = Set{Symbol}()
    for pd in products; union!(prod_set, keys(pd)); end

    free_syms = sort([s for s in all_species if s ∉ prod_set])
    prod_syms = sort([s for s in prod_set])
    species   = vcat(free_syms, prod_syms)
    n = length(species)
    idx = Dict(s => i for (i, s) in enumerate(species))

    N = zeros(Int, r, n)
    for i in 1:r
        for (s, c) in reactants[i]; N[i, idx[s]] += c; end
        for (s, c) in products[i];  N[i, idx[s]] -= c; end
    end
    return N, species, free_syms, prod_syms
end

"""Build a Bnc model from `<->` rule strings, mirroring webapp build_model."""
function _fixture_build_model(rules::Vector{String})
    N, species, free_syms, prod_syms = _build_N(rules)
    r = length(rules)
    x_sym_v = Symbol.(species)
    q_sym_v = Symbol.("t" .* String.(free_syms))
    K_sym_v = Symbol.("Kd" .* string.(1:r))
    model = Bnc(N=N, x_sym=x_sym_v, q_sym=q_sym_v, K_sym=K_sym_v)
    return model
end

# ── Fixture definitions ───────────────────────────────────────────────────────

"""
Return the canonical fixture list.

Each element: (name, model, pairs, output_expr, param_idx, param_range).

`pairs` are (x_symbol, qK_symbol) tuples for get_ROP_plot_data.
`param_idx` and `param_range` are for scan_parameter_1d.
`output_expr` is parseable via parse_linear_combination(model, output_expr).

Networks sourced verbatim from Bnc_julia/test/runtests.jl.
"""
function fixtures()::Vector{NamedTuple}
    result = NamedTuple[]

    # ── 1. single: L + A <-> AL ──────────────────────────────────────────────
    # qK_sym = [tA, tL, Kd1];  x_sym = [A, L, AL]
    # Sweep tL (param_idx=2); output = AL; ROP axes: AL vs tL, AL vs Kd1
    let model = _fixture_build_model(["L + A <-> AL"])
        push!(result, (
            name        = "single",
            model       = model,
            pairs       = [(:AL, :tL), (:AL, :Kd1)],
            output_expr = "AL",
            param_idx   = 2,   # tL is index 2 in [tA, tL, Kd1]
            param_range = collect(range(-3.0, 3.0; length=121)),
        ))
    end

    # ── 2. competitive: L+A<->AL ; L+B<->BL ─────────────────────────────────
    # qK_sym = [tA, tB, tL, Kd1, Kd2];  x_sym = [A, B, L, AL, BL]
    # Sweep tL (param_idx=3); output = AL; ROP axes: AL vs tL, AL vs Kd1
    let model = _fixture_build_model(["L + A <-> AL", "L + B <-> BL"])
        push!(result, (
            name        = "competitive",
            model       = model,
            pairs       = [(:AL, :tL), (:AL, :Kd1)],
            output_expr = "AL",
            param_idx   = 3,   # tL is index 3 in [tA, tB, tL, Kd1, Kd2]
            param_range = collect(range(-3.0, 3.0; length=121)),
        ))
    end

    # ── 3. cooperative / two-site: A+L<->AL ; AL+L<->AL2 ────────────────────
    # qK_sym = [tA, tL, Kd1, Kd2];  x_sym = [A, L, AL, AL2]
    # Sweep tL (param_idx=2); output = AL2; ROP axes: AL2 vs tL, AL2 vs Kd1
    let model = _fixture_build_model(["A + L <-> AL", "AL + L <-> AL2"])
        push!(result, (
            name        = "cooperative",
            model       = model,
            pairs       = [(:AL2, :tL), (:AL2, :Kd1)],
            output_expr = "AL2",
            param_idx   = 2,   # tL is index 2 in [tA, tL, Kd1, Kd2]
            param_range = collect(range(-3.0, 3.0; length=121)),
        ))
    end

    # ── 4. prozone / hook: L+A<->AL ; L+B<->BL ; AL+B<->ALB ─────────────────
    # qK_sym = [tA, tB, tL, Kd1, Kd2, Kd3];  x_sym = [A, B, L, AL, ALB, BL]
    # Sweep tL (param_idx=3); output = ALB; ROP axes: ALB vs tL, ALB vs Kd1
    let model = _fixture_build_model(["L + A <-> AL", "L + B <-> BL", "AL + B <-> ALB"])
        push!(result, (
            name        = "prozone",
            model       = model,
            pairs       = [(:ALB, :tL), (:ALB, :Kd1)],
            output_expr = "ALB",
            param_idx   = 3,   # tL is index 3 in [tA, tB, tL, Kd1, Kd2, Kd3]
            param_range = collect(range(-3.0, 3.0; length=121)),
        ))
    end

    # ── 5. sequential ternary: A+L<->AL ; AL+B<->ALB ─────────────────────────
    # qK_sym = [tA, tB, tL, Kd1, Kd2];  x_sym = [A, B, L, AL, ALB]
    # Sweep tL (param_idx=3); output = ALB; ROP axes: ALB vs tL, ALB vs Kd2
    let model = _fixture_build_model(["A + L <-> AL", "AL + B <-> ALB"])
        push!(result, (
            name        = "ternary",
            model       = model,
            pairs       = [(:ALB, :tL), (:ALB, :Kd2)],
            output_expr = "ALB",
            param_idx   = 3,   # tL is index 3 in [tA, tB, tL, Kd1, Kd2]
            param_range = collect(range(-3.0, 3.0; length=121)),
        ))
    end

    return result
end
