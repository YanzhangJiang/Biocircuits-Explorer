# ─── Canonicalization & content-identity primitives ───────────────────────────
#
# The single home for the helpers that turn loosely-typed JSON values and binding
# networks into *stable, content-addressable identities*. Everything that hashes
# an artifact (ir.jl `network_ir_hash`/`design_spec_hash`, result_artifact.jl,
# inverse_design.jl `stable_hash`/`h_Q`, atlas.jl `canonical_network_code`) now
# routes through here, so there is ONE canonicalization implementation rather than
# the previous three (this file was extracted so the IR/result substrate no longer
# depends *backwards* on the 200KB atlas.jl / 126KB inverse_design.jl just to hash).
#
# Flat-included into BiocircuitsExplorerBackend BEFORE atlas.jl/ir.jl. Depends only
# on SHA, JSON3, Dates (parent `using`s) and `ReactionParser` (parse_reactions /
# parse_network_structure) for the network-canonical path; function bodies resolve
# those names at call time, so include order within the parent module is safe.

# ── Loosely-typed JSON access (was in atlas.jl) ───────────────────────────────

function _raw_haskey(raw, key::Symbol)
    return haskey(raw, key) || haskey(raw, String(key))
end

function _raw_get(raw, key::Symbol, default)
    if haskey(raw, key)
        return raw[key]
    elseif haskey(raw, String(key))
        return raw[String(key)]
    else
        return default
    end
end

_now_iso_timestamp() = Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS")

function _materialize(value)
    if value isa AbstractDict
        out = Dict{String, Any}()
        for (k, v) in pairs(value)
            out[String(k)] = _materialize(v)
        end
        return out
    elseif value isa AbstractVector || value isa Tuple
        return Any[_materialize(v) for v in value]
    elseif value isa Symbol
        return String(value)
    else
        return value
    end
end

# ── Canonical JSON for hashing ────────────────────────────────────────────────
#
# IMPORTANT: `JSON3.write(dict)` emits keys in the Dict's hash-bucket iteration
# order, NOT sorted order — so the old "sort keys into a fresh Dict, then
# JSON3.write" trick produced a hash that was only *incidentally* stable (same key
# set + same Julia version happened to lay out identically). `_canonical_json`
# instead builds the JSON string itself with keys emitted in sorted order, giving
# a genuinely canonical serialization that is robust across Julia versions and
# Dict construction order.
#
# It also normalizes numbers: integral floats (e.g. `1.0`) serialize identically
# to the integer `1`, so a value that arrives as Int in one payload and Float in
# another (common in the free-form `extensions`/metadata bags) hashes the same.
# Non-finite floats are rejected — they are not valid JSON and would hash
# inconsistently.
function _canonical_json(value)::String
    if value isa AbstractDict
        keystrs = sort!(String[String(k) for k in keys(value)])
        parts = String[]
        for ks in keystrs
            # look up by whatever key type the dict actually uses
            v = haskey(value, ks) ? value[ks] : value[Symbol(ks)]
            push!(parts, JSON3.write(ks) * ":" * _canonical_json(v))
        end
        return "{" * join(parts, ",") * "}"
    elseif value isa AbstractVector || value isa Tuple
        return "[" * join((_canonical_json(v) for v in value), ",") * "]"
    elseif value isa Symbol
        return JSON3.write(String(value))
    elseif value isa Bool
        return value ? "true" : "false"
    elseif value isa Integer
        return string(value)
    elseif value isa AbstractFloat
        (isnan(value) || isinf(value)) &&
            throw(ArgumentError("non-finite number cannot be canonicalized: $value"))
        return isinteger(value) ? string(Integer(value)) : JSON3.write(Float64(value))
    elseif value === nothing
        return "null"
    elseif value isa AbstractString
        return JSON3.write(String(value))
    else
        return JSON3.write(value)   # exotic types: at least stable via JSON3
    end
end

"SHA-256 hex over the canonical JSON of any value (content-identity / cache key)."
canonical_hash(value) = bytes2hex(SHA.sha256(_canonical_json(value)))

"SHA-1 hex over the canonical JSON; kept for inverse_design's short (`[1:12]`) ids."
canonical_hash_sha1(value) = bytes2hex(SHA.sha1(_canonical_json(value)))

# Back-compat shim: a few callers passed `_stable_canonical_value` through
# `JSON3.write`. They now call `canonical_hash` directly, but keep this defined as
# the normalized-value form in case future non-hashing callers want it.
_stable_canonical_value(value) =
    value isa AbstractDict ? Dict{String,Any}(String(k) => _stable_canonical_value(v) for (k,v) in value) :
    (value isa AbstractVector || value isa Tuple) ? Any[_stable_canonical_value(v) for v in value] :
    value isa Symbol ? String(value) : value

stable_hash(value) = canonical_hash_sha1(value)

# ── Binding-network canonical code (graph-canonical topology identity) ────────
#
# Was in atlas.jl. This is the project's STRONGEST network identity: it is
# invariant to reaction reordering and to renaming of the free (base) species,
# because it minimizes the serialized form over all permutations of the base
# symbols. The CAD layer's `network_ir_hash` now derives its structural identity
# from this same function (see ir.jl), so the atlas and the IR/CAD layer share a
# single notion of "the same network" instead of two disjoint ones.

function _all_permutations(items::Vector{T}) where {T}
    length(items) <= 1 && return [copy(items)]
    perms = Vector{Vector{T}}()
    for idx in eachindex(items)
        head = items[idx]
        tail = T[items[j] for j in eachindex(items) if j != idx]
        for perm in _all_permutations(tail)
            push!(perms, vcat(T[head], perm))
        end
    end
    return perms
end

function _merge_support_signature!(dest::Vector{Symbol}, src::Vector{Symbol}, copies::Integer=1)
    copies <= 0 && return dest
    for _ in 1:copies
        append!(dest, src)
    end
    return dest
end

function _infer_binding_supports(rules::Vector{String})
    reactants, products = parse_reactions(rules)
    _, _, free_syms, prod_syms = parse_network_structure(rules)
    supports = Dict{Symbol, Vector{Symbol}}(sym => [sym] for sym in free_syms)

    progress = true
    while progress
        progress = false
        for idx in eachindex(rules)
            reactant_dict = reactants[idx]
            product_dict = products[idx]
            if length(product_dict) != 1 || any(coeff != 1 for coeff in values(product_dict))
                continue
            end
            all(haskey(supports, sym) for sym in keys(reactant_dict)) || continue

            product_sym = first(keys(product_dict))
            merged = Symbol[]
            for sym in sort!(collect(keys(reactant_dict)))
                coeff = reactant_dict[sym]
                _merge_support_signature!(merged, supports[sym], coeff)
            end
            sort!(merged)

            if !haskey(supports, product_sym)
                supports[product_sym] = merged
                progress = true
            elseif supports[product_sym] != merged
                return nothing, "inconsistent_support_assignment:$(product_sym)"
            end
        end
    end

    missing = Symbol[sym for sym in prod_syms if !haskey(supports, sym)]
    if !isempty(missing)
        return nothing, "support_inference_failed:" * join(sort(string.(missing)), ",")
    end

    return supports, nothing
end

function _canonical_term_string(sym::Symbol, supports::Dict{Symbol, Vector{Symbol}}, remap::Dict{Symbol, Int})
    term = sort(collect(remap[base] for base in supports[sym]))
    return "[" * join(term, ",") * "]"
end

function canonical_network_code(rules::Vector{String})
    reactants, products = parse_reactions(rules)
    _, _, free_syms, _ = parse_network_structure(rules)
    supports, support_issue = _infer_binding_supports(rules)
    support_issue === nothing || error("Cannot canonicalize network: $support_issue")

    candidates = String[]
    for perm in _all_permutations(copy(free_syms))
        remap = Dict(sym => idx for (idx, sym) in enumerate(perm))
        serialized_rules = String[]
        for idx in eachindex(rules)
            left_terms = String[]
            for (sym, coeff) in reactants[idx]
                term = _canonical_term_string(sym, supports, remap)
                for _ in 1:coeff
                    push!(left_terms, term)
                end
            end
            right_terms = String[]
            for (sym, coeff) in products[idx]
                term = _canonical_term_string(sym, supports, remap)
                for _ in 1:coeff
                    push!(right_terms, term)
                end
            end
            sort!(left_terms)
            sort!(right_terms)
            push!(serialized_rules, join(left_terms, "+") * "<->" * join(right_terms, "+"))
        end
        sort!(serialized_rules)
        push!(candidates, join(serialized_rules, "|"))
    end

    sort!(candidates)
    return first(candidates)
end

"""
`canonical_network_code` that returns `nothing` instead of throwing when the
network is not canonicalizable (e.g. non-binding / catalysis out of grammar).
"""
function canonical_network_code_or_nothing(rules::Vector{String})
    try
        return canonical_network_code(rules)
    catch
        return nothing
    end
end

"""
    topology_family_key(rules) -> String

A graph-invariant TOPOLOGY-FAMILY key, coarser than `canonical_network_code`:
`(#base species, #reactions, sorted base-species degree sequence)`, where a base
species' degree is the number of complexes whose support contains it. Networks
with the same structural "shape profile" share a family even if they are not
isomorphic, so holding out a whole family tests generalization to unseen topology
families (the roadmap's family-holdout split) — unlike `canonical_network_code`,
which is per-network and would make family-holdout equivalent to IID. Falls back
to a per-network key when binding supports cannot be inferred.
"""
function topology_family_key(rules::Vector{String})
    supports, issue = _infer_binding_supports(rules)
    issue === nothing || return "uninferable:" * canonical_hash_sha1(rules)[1:8]
    _, _, free_syms, prod_syms = parse_network_structure(rules)
    deg = Dict(s => 0 for s in free_syms)
    for p in prod_syms
        for b in get(supports, p, Symbol[])
            haskey(deg, b) && (deg[b] += 1)
        end
    end
    degseq = sort(collect(values(deg)))
    return "b$(length(free_syms))_r$(length(rules))_d[" * join(degseq, ",") * "]"
end
