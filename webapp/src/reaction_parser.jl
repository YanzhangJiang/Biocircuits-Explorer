module ReactionParser

using BindingAndCatalysis

const ARROW_RE = r"<->|<=>|↔"

function parse_term(term::AbstractString)
    t = strip(term)
    isempty(t) && error("Empty term")
    # Keep the grammar aligned with SBML SId: a leading underscore is valid.
    m = match(r"^([0-9]+)?\s*([A-Za-z_][A-Za-z0-9_]*)$", t)
    m === nothing && error("Bad term: $term")
    coeff = m.captures[1] === nothing ? 1 : parse(Int, m.captures[1])
    sym = Symbol(m.captures[2])
    return sym, coeff
end

function parse_side(side::AbstractString)
    parts = split(side, "+")
    dict = Dict{Symbol,Int}()
    for p in parts
        sym, coeff = parse_term(p)
        dict[sym] = get(dict, sym, 0) + coeff
    end
    return dict
end

function parse_reactions(rules::Vector{String})
    reactants = Vector{Dict{Symbol,Int}}()
    products  = Vector{Dict{Symbol,Int}}()
    for rule in rules
        m = match(ARROW_RE, rule)
        m === nothing && error("Reaction must contain '<->' or '<=>' or '↔': $rule")
        left, right = split(rule, m.match)
        push!(reactants, parse_side(left))
        push!(products, parse_side(right))
    end
    return reactants, products
end

function parse_network_structure(rules::Vector{String})
    reactants, products = parse_reactions(rules)
    r = length(rules)

    all_species = Set{Symbol}()
    for rd in reactants
        union!(all_species, keys(rd))
    end
    for pd in products
        union!(all_species, keys(pd))
    end

    # Species appearing on the product side are treated as bound species.
    prod_species_set = Set{Symbol}()
    for pd in products
        union!(prod_species_set, keys(pd))
    end
    free_syms = sort([s for s in all_species if s ∉ prod_species_set])
    prod_syms = sort([s for s in prod_species_set])

    # Free species first, then bound species.
    species = vcat(free_syms, prod_syms)
    n = length(species)
    idx = Dict(s => i for (i, s) in enumerate(species))

    # N matrix (r × n); each row follows the reactants − products log-space sign convention.
    N = zeros(Int, r, n)
    for i in 1:r
        for (s, coeff) in reactants[i]
            N[i, idx[s]] += coeff
        end
        for (s, coeff) in products[i]
            N[i, idx[s]] -= coeff
        end
    end

    return N, species, free_syms, prod_syms
end

function build_model(rules::Vector{String}, kd::Vector{Float64})
    r = length(rules)
    length(kd) == r || error("Length(kd) must match number of reactions")
    all(value -> isfinite(value) && value > 0, kd) ||
        error("All Kd values must be finite and positive (> 0)")

    N, species, free_syms, prod_syms = parse_network_structure(rules)

    x_sym = Symbol.(species)
    q_sym = Symbol.("t" .* String.(free_syms))
    K_sym = Symbol.("Kd" .* string.(1:r))

    # Bnc's constructor validates n == d + r and that N is linearly independent.
    model = Bnc(N=N, x_sym=x_sym, q_sym=q_sym, K_sym=K_sym)
    return model, species, free_syms, prod_syms
end

function default_log_qK(model, kd::AbstractVector{<:Real}; default_logq::Real = 0.0)
    all(value -> !(value isa Bool), kd) ||
        error("All Kd values must be finite, non-boolean, and positive (> 0).")
    default_logq isa Bool && error("default_logq must be a finite non-boolean number.")
    isfinite(default_logq) || error("default_logq must be a finite non-boolean number.")
    kd_vec = Float64.(collect(kd))
    length(kd_vec) == model.r ||
        error("Length of Kd values ($(length(kd_vec))) must match model reaction dimension ($(model.r)).")
    all(x -> isfinite(x) && x > 0, kd_vec) ||
        error("All Kd values must be finite and positive (> 0).")
    return vcat(fill(Float64(default_logq), model.d), log10.(kd_vec))
end

function fixed_qK_or_default(body, model, kd::AbstractVector{<:Real})
    fixed_qK = if haskey(body, :fixed_qK)
        raw = body[:fixed_qK]
        raw isa AbstractVector || error("`fixed_qK` must be an array of finite non-boolean numbers.")
        all(value -> value isa Real && !(value isa Bool), raw) ||
            error("`fixed_qK` must be an array of finite non-boolean numbers.")
        values = Float64.(raw)
        all(isfinite, values) ||
            error("`fixed_qK` must be an array of finite non-boolean numbers.")
        values
    else
        default_log_qK(model, kd)
    end
    length(fixed_qK) == model.n ||
        error("Length of `fixed_qK` must equal the full q/K dimension ($(model.n)).")
    return fixed_qK
end

end # module
