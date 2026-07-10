# ════════ DESIGN PIPELINE: target → designable? → screen → minimal ════════
# Query the bundled enumerated atlas (slices.jsonl.gz, the complete d≤4/μ≤5 product)
# for networks whose reaction-order program matches a target, and return the
# Pareto-minimal (d,r,μ). `design_screen` adds a tunability-aware candidate
# screening layer without folding model building or placement into this node.
const _DESIGN_INDEX_STATE = Ref{Any}(nothing)
const _DESIGN_INDEX_LOCK = ReentrantLock()
const _DESIGN_INDEX_LOAD_COUNT = Ref(0) # guarded by _DESIGN_INDEX_LOCK; test/diagnostic hook

_design_canonical_ro(value::Real) = begin
    rounded = round(Float64(value), digits=3)
    iszero(rounded) ? 0.0 : rounded
end

function _design_collapse(progstr::AbstractString)
    toks = Float64[]
    for t in split(progstr, r"→|->|,")
        s = strip(t)
        (isempty(s) || occursin("Inf", s) || occursin("NaN", s)) && continue
        v = tryparse(Float64, s); v === nothing && continue
        push!(toks, _design_canonical_ro(v))
    end
    out = Float64[]
    for v in toks; (isempty(out) || out[end] != v) && push!(out, v); end
    return out
end
function _design_sign(prog::Vector{Float64})
    out = Char[]
    for v in prog
        v == 0 && continue
        c = v > 0 ? '+' : '-'
        (isempty(out) || out[end] != c) && push!(out, c)
    end
    return String(out)
end

function _design_species_labels(sym::AbstractString)
    text = strip(String(sym))
    isempty(text) && return String[]
    if startswith(text, "C_")
        return [String(label) for label in split(text[3:end], "_")]
    end
    return [text]
end

function _design_species_symbol(labels::AbstractVector{<:AbstractString})
    sorted_labels = sort(String.(labels))
    isempty(sorted_labels) && return ""
    length(sorted_labels) == 1 && return first(sorted_labels)
    return "C_" * join(sorted_labels, "_")
end

function _design_label_index(label::AbstractString)
    text = String(label)
    if length(text) == 1
        ch = only(text)
        ('A' <= ch <= 'Z') && return Int(ch - 'A') + 1
    end
    if startswith(text, "S")
        idx = tryparse(Int, text[2:end])
        idx === nothing || return idx
    end
    return nothing
end

function _design_canonical_species(nid::AbstractString)
    species = Set{String}()
    max_idx = 0
    for reaction in split(String(nid), "|")
        parts = split(reaction, "<->")
        length(parts) == 2 || continue
        for side in parts
            for term in _atlas_sqlite_parse_canonical_side(side)
                isempty(term) && continue
                max_idx = max(max_idx, maximum(term))
                push!(species, _design_species_symbol([_base_species_label(Int(idx)) for idx in term]))
            end
        end
    end
    return species, max_idx
end

function _design_symbol_max_index(symbols)
    max_idx = 0
    for sym in symbols
        for label in _design_species_labels(String(sym))
            idx = _design_label_index(label)
            idx === nothing && continue
            max_idx = max(max_idx, idx)
        end
    end
    return max_idx
end

function _design_permutation_orders(n::Int)
    n <= 0 && return [Int[]]
    n > 7 && return [collect(1:n)]
    out = Vector{Vector{Int}}()
    used = falses(n)
    current = Int[]
    function visit!()
        if length(current) == n
            push!(out, copy(current))
            return
        end
        for idx in 1:n
            used[idx] && continue
            used[idx] = true
            push!(current, idx)
            visit!()
            pop!(current)
            used[idx] = false
        end
    end
    visit!()
    return out
end

function _design_apply_species_map(label_map::Dict{String,String}, sym::AbstractString)
    labels = _design_species_labels(sym)
    isempty(labels) && return String(sym)
    return _design_species_symbol([get(label_map, label, label) for label in labels])
end

function _design_apply_total_map(label_map::Dict{String,String}, sym::AbstractString)
    text = String(sym)
    m = match(r"^t(.+)$", text)
    m === nothing && return text
    label = String(m.captures[1])
    return "t" * get(label_map, label, label)
end

function _design_symbol_map_for_nid(nid::AbstractString, raw_outputs::Set{String})
    canonical_species, canonical_max = _design_canonical_species(nid)
    isempty(canonical_species) && return Dict{String,String}()
    n = max(canonical_max, _design_symbol_max_index(raw_outputs))
    labels = [_base_species_label(idx) for idx in 1:n]
    best_map = Dict(label => label for label in labels)
    best_score = -1
    best_extra = typemax(Int)
    best_moves = typemax(Int)
    for order in _design_permutation_orders(n)
        label_map = Dict(labels[idx] => labels[order[idx]] for idx in 1:n)
        mapped_outputs = Set(_design_apply_species_map(label_map, out) for out in raw_outputs)
        score = count(out -> out in canonical_species, mapped_outputs)
        extra = count(out -> !(out in canonical_species), mapped_outputs)
        moves = count(idx -> order[idx] != idx, 1:n)
        if score > best_score ||
           (score == best_score && extra < best_extra) ||
           (score == best_score && extra == best_extra && moves < best_moves)
            best_map = label_map
            best_score = score
            best_extra = extra
            best_moves = moves
        end
    end
    return best_map
end

function _load_design_index_state()
    return lock(_DESIGN_INDEX_LOCK) do
        # Exactly one request performs the 335 MB decompression/parse. Records
        # and reverse indexes are published together as one immutable state.
        _DESIGN_INDEX_STATE[] === nothing || return _DESIGN_INDEX_STATE[]
        _DESIGN_INDEX_LOAD_COUNT[] += 1
    # Tracked new-sign atlas (147,081 slices, carries `base_motifs`). The old
    # webapp/data copy is stale (146,845, old singular-sign); BNE_DESIGN_INDEX overrides.
    path = get(ENV, "BNE_DESIGN_INDEX",
               normpath(joinpath(@__DIR__, "..", "..", "paper_rop_periodic_table",
                                 "data", "slices.jsonl.gz")))
    isfile(path) || error("design index (slices.jsonl.gz) not found at $path")
    raw_recs = NamedTuple[]
    raw_outputs_by_nid = Dict{String, Set{String}}()
    open(`gzip -dc $path`) do io
        for line in eachline(io)
            isempty(strip(line)) && continue
            j = JSON3.read(line)
            ex = get(j, :exact, nothing); ex === nothing && continue
            progs = Vector{Float64}[]
            for p in ex
                cp = _design_collapse(String(p)); isempty(cp) || push!(progs, cp)
            end
            isempty(progs) && continue
            bms = get(j, :base_motifs, nothing)
            motifs = bms === nothing ? Set{String}() : Set(String.(bms))
            nid = String(get(j, :nid, ""))
            inp = String(get(j, :inp, ""))
            out = String(get(j, :out, ""))
            push!(raw_recs, (; d = Int(j.d), r = Int(j.r), mu = Int(j.mu),
                              nid = nid, inp = inp, out = out,
                              signs = Set(_design_sign(p) for p in progs), exacts = Set(progs),
                              motifs = motifs,
                              feasible_paths = Int(get(j, :feasible_paths, 0)),
                              total_paths = Int(get(j, :total_paths, 0)),
                              n_species = Int(get(j, :n_species, 0)),
                              n_complexes = Int(get(j, :n_complexes, 0)),
                              max_complex_size = Int(get(j, :max_complex_size, 0)),
                              assembly_depth = Int(get(j, :assembly_depth, 0)),
                              uses_homomer = Int(get(j, :uses_homomer, 0)),
                              uses_complex_growth = Int(get(j, :uses_complex_growth, 0)),
                              uses_higher_order_template = Int(get(j, :uses_higher_order_template, 0)),
                              sl_nswitch = Int(get(j, :sl_nswitch, 0)),
                              sl_absro = Float64(get(j, :sl_absro, 0.0)),
                              vol_sum = Float64(get(j, :vol_sum, 0.0)),
                              vol_max = Float64(get(j, :vol_max, 0.0)),
                              robust_paths = Int(get(j, :robust_paths, 0))))
            if !isempty(nid) && !isempty(out)
                push!(get!(raw_outputs_by_nid, nid, Set{String}()), out)
            end
        end
    end
    symbol_maps = Dict(nid => _design_symbol_map_for_nid(nid, outs)
                       for (nid, outs) in raw_outputs_by_nid)
    # Normalize in place so cold start never retains both a complete raw and a
    # complete normalized 147k-record vector at once.
    for idx in eachindex(raw_recs)
        rec = raw_recs[idx]
        label_map = get(symbol_maps, rec.nid, Dict{String,String}())
        raw_recs[idx] = (; d = rec.d, r = rec.r, mu = rec.mu,
                           nid = rec.nid,
                           inp = _design_apply_total_map(label_map, rec.inp),
                           out = _design_apply_species_map(label_map, rec.out),
                           signs = rec.signs, exacts = rec.exacts, motifs = rec.motifs,
                           feasible_paths = rec.feasible_paths, total_paths = rec.total_paths,
                           n_species = rec.n_species, n_complexes = rec.n_complexes,
                           max_complex_size = rec.max_complex_size,
                           assembly_depth = rec.assembly_depth,
                           uses_homomer = rec.uses_homomer,
                           uses_complex_growth = rec.uses_complex_growth,
                           uses_higher_order_template = rec.uses_higher_order_template,
                           sl_nswitch = rec.sl_nswitch, sl_absro = rec.sl_absro,
                           vol_sum = rec.vol_sum, vol_max = rec.vol_max,
                           robust_paths = rec.robust_paths)
    end
    recs = raw_recs
    by_sign = Dict{String, Vector{Int}}()
    by_label = Dict{String, Vector{Int}}()
    by_exact = Dict{Tuple{Vararg{Float64}}, Vector{Int}}()
    for (idx, rec) in enumerate(recs)
        for sign in rec.signs
            push!(get!(by_sign, sign, Int[]), idx)
        end
        for label in rec.motifs
            push!(get!(by_label, label, Int[]), idx)
        end
        for exact in rec.exacts
            push!(get!(by_exact, Tuple(Float64.(exact)), Int[]), idx)
        end
    end
    state = (; records=recs, lookup=(; by_sign, by_label, by_exact))
    _DESIGN_INDEX_STATE[] = state
        return state
    end
end

_load_design_index() = _load_design_index_state().records
function _design_pareto(cells)
    cs = sort(collect(Set(cells)))
    [c for c in cs if !any(o != c && o[1] <= c[1] && o[2] <= c[2] && o[3] <= c[3] for o in cs)]
end

function _design_normalize_design_target(target_kind, target)
    target === nothing && error("`target` is required")
    kind = String(target_kind)
    kind in ("sign", "exact", "label") ||
        error("target_kind must be one of `sign`, `exact`, or `label`")
    if kind == "exact"
        target isa AbstractString && error("exact target must be an array of numbers")
        length(target) <= 32 || error("exact target may contain at most 32 values")
        vals = Float64[]
        for value in collect(target)
            (value isa Bool || !(value isa Real)) &&
                error("exact target values must be finite non-Bool numbers")
            f = Float64(value)
            isfinite(f) || error("exact target values must be finite non-Bool numbers")
            rounded = _design_canonical_ro(f)
            (isempty(vals) || vals[end] != rounded) && push!(vals, rounded)
        end
        isempty(vals) && error("exact target must contain at least one number")
        return kind, vals
    else
        text = strip(String(target))
        isempty(text) && error("`target` must not be empty")
        ncodeunits(text) <= (kind == "sign" ? 32 : 64) ||
            error("$kind target is too long")
        if kind == "sign"
            all(c -> c == '+' || c == '-', text) ||
                error("sign target must contain only `+` and `-`")
            collapsed = Char[]
            for c in text
                (isempty(collapsed) || collapsed[end] != c) && push!(collapsed, c)
            end
            text = String(collapsed)
        end
        return kind, text
    end
end

function _design_matches_normalized(target_kind::AbstractString, target)
    state = _load_design_index_state()
    idx = state.records
    lookup = state.lookup
    record_indices = if target_kind == "label"
        get(lookup.by_label, String(target), Int[])
    elseif target_kind == "sign"
        get(lookup.by_sign, String(target), Int[])
    else
        tgt = Tuple(_design_canonical_ro(x) for x in target)
        get(lookup.by_exact, tgt, Int[])
    end
    return [idx[i] for i in record_indices]
end

function _design_matches(target_kind::AbstractString, target)
    kind, normalized_target = _design_normalize_design_target(target_kind, target)
    return _design_matches_normalized(kind, normalized_target)
end

function _design_search_response(matched, cells)
    pm = _design_pareto(cells)
    minimal = Dict[]
    for c in pm
        nets = [Dict("nid" => m.nid, "inp" => m.inp, "out" => m.out)
                for m in matched if (m.d, m.r, m.mu) == c]
        push!(minimal, Dict("d" => c[1], "r" => c[2], "mu" => c[3],
                            "networks" => nets[1:min(length(nets), 8)]))
    end
    return Dict("designable" => !isempty(matched), "n_matches" => length(matched),
                "minimal" => minimal,
                "all_cells" => sort([[c[1], c[2], c[3]] for c in Set(cells)]))
end

# target_kind: "sign" (e.g. "+-+"), "exact" (Vector of numbers, e.g. [1.5,0,-1]),
# or "label" (a behavior base-motif, e.g. "biphasic_peak" — matched against each
# slice's precomputed `base_motifs`, so no classifier is needed).
function design_search(target_kind::AbstractString, target)
    kind, normalized_target = _design_normalize_design_target(target_kind, target)
    matched = _design_matches_normalized(kind, normalized_target)
    cells = [(m.d, m.r, m.mu) for m in matched]
    return _design_search_response(matched, cells)
end

function _design_float(raw, key::Symbol, default::Real)
    value = _raw_get(raw, key, default)
    value === nothing && return Float64(default)
    return Float64(value)
end

function _design_int(raw, key::Symbol, default::Integer)
    value = _raw_get(raw, key, default)
    value === nothing && return Int(default)
    return Int(value)
end

function _design_dict_or_empty(raw)
    raw === nothing && return Dict{String, Any}()
    try
        keys(raw)
        return raw
    catch
        return Dict{String, Any}()
    end
end

function _design_bounds(raw, key::Symbol, default::Tuple{Float64, Float64})
    value = _raw_get(raw, key, nothing)
    value === nothing && return default
    vals = collect(value)
    length(vals) == 2 || error("`$key` must have two values")
    lo, hi = Float64(vals[1]), Float64(vals[2])
    lo < hi || error("`$key` lower bound must be < upper bound")
    return (lo, hi)
end

function _design_candidate_key(rec)
    return string(rec.nid, "::", rec.inp, "::", rec.out)
end

function _design_better_record(a, b)
    a_score = (Float64(a.vol_max), Int(a.robust_paths), Float64(a.vol_sum), -Int(a.r), -Int(a.d))
    b_score = (Float64(b.vol_max), Int(b.robust_paths), Float64(b.vol_sum), -Int(b.r), -Int(b.d))
    return a_score > b_score ? a : b
end

function _design_unique_records(records)
    by_key = Dict{String, Any}()
    for rec in records
        key = _design_candidate_key(rec)
        by_key[key] = haskey(by_key, key) ? _design_better_record(rec, by_key[key]) : rec
    end
    return [by_key[key] for key in sort(collect(keys(by_key)))]
end

function _design_near_minimal(rec, pareto_cells, max_extra_d::Int, max_extra_r::Int, max_extra_mu::Int)
    isempty(pareto_cells) && return false
    any(c -> rec.d <= c[1] + max_extra_d &&
             rec.r <= c[2] + max_extra_r &&
             rec.mu <= c[3] + max_extra_mu, pareto_cells)
end

function _design_total_symbols(d::Integer)
    return ["t" * _base_species_label(idx) for idx in 1:Int(d)]
end

function _design_complex_token(term)
    labels = [_base_species_label(Int(idx)) for idx in term]
    isempty(labels) && return ""
    length(labels) == 1 && return first(labels)
    return "C_" * join(labels, "_")
end

function _design_side_to_rule_species(side::AbstractString)
    terms = _atlas_sqlite_parse_canonical_side(side)
    return join([_design_complex_token(term) for term in terms], " + ")
end

function _design_nid_to_rules(nid::AbstractString)
    rules = String[]
    for reaction in split(String(nid), "|")
        parts = split(reaction, "<->")
        length(parts) == 2 || error("Cannot parse reaction in nid: $reaction")
        push!(rules, string(_design_side_to_rule_species(parts[1]), " <-> ",
                            _design_side_to_rule_species(parts[2])))
    end
    return rules
end

function _design_target_transition_count(rec)
    longest = 1
    for prog in rec.exacts
        longest = max(longest, length(prog))
    end
    return max(Int(rec.sl_nswitch), longest - 1, 1)
end

function _design_screen_card(rec, pareto_cell_set, tuning, ranking_policy)
    kd_bounds = _design_bounds(tuning, :kd_bounds, (-3.0, 3.0))
    total_bounds = _design_bounds(tuning, :total_bounds, (-3.0, 3.0))
    min_dynamic_range = _design_float(tuning, :min_dynamic_range, 0.0)
    min_transition_spacing = _design_float(tuning, :min_transition_spacing_decades, 0.0)
    min_margin_radius = _design_float(tuning, :min_chebyshev_radius, 0.0)

    kd_span = kd_bounds[2] - kd_bounds[1]
    transition_count = _design_target_transition_count(rec)
    transition_spacing = kd_span / max(transition_count, 1)
    volume_radius = rec.vol_max > 0 ? rec.vol_max^(1 / max(rec.r, 1)) : 0.0
    cheb_radius = min(kd_span / 2, transition_spacing / 2, volume_radius)
    dynamic_range = max(0.0, Float64(rec.sl_absro)) + log10(1 + max(Int(rec.robust_paths), 0))
    tunable_volume = max(Float64(rec.vol_max), 0.0)
    volume_sum = max(Float64(rec.vol_sum), 0.0)
    sampled_robustness = max(Int(rec.robust_paths), 0) / max(max(Int(rec.robust_paths), Int(rec.total_paths)), 1)
    parameter_extremeness = rec.r / max(kd_span, eps(Float64))
    condition_number = 1.0 + rec.r / max(rec.robust_paths, 1) + transition_count / max(kd_span, eps(Float64))
    sensitivity = 1.0 / max(transition_spacing, eps(Float64))

    active_failures = String[]
    dynamic_range < min_dynamic_range && push!(active_failures, "dynamic_range_below_min")
    transition_spacing < min_transition_spacing && push!(active_failures, "transition_spacing_below_min")
    cheb_radius < min_margin_radius && push!(active_failures, "chebyshev_radius_below_min")
    rec.vol_max <= 0 && push!(active_failures, "no_atlas_volume_proxy")
    pass = isempty(active_failures)

    radius_norm = clamp(cheb_radius / max(kd_span / 2, eps(Float64)), 0.0, 1.0)
    volume_norm = clamp(tunable_volume, 0.0, 1.0)
    spacing_norm = clamp(transition_spacing / max(kd_span, eps(Float64)), 0.0, 1.0)
    dynamic_norm = clamp(dynamic_range / max(min_dynamic_range, 1.0), 0.0, 1.0)
    complexity_penalty = clamp(0.03 * rec.r + 0.02 * rec.d + 0.02 * rec.mu, 0.0, 0.5)
    score = clamp(0.34 * radius_norm + 0.22 * volume_norm + 0.16 * sampled_robustness +
                  0.12 * spacing_norm + 0.10 * dynamic_norm - complexity_penalty, 0.0, 1.0)

    totals = Dict(sym => 1.0 for sym in _design_total_symbols(rec.d))
    kd = fill(1.0, rec.r)
    log_qK = fill(0.0, rec.d + rec.r)
    minimal = (rec.d, rec.r, rec.mu) in pareto_cell_set
    grade = rec.vol_max > 0 ? "atlas-volume-proxy" : "screened-surrogate"

    return Dict(
        "schema_version" => "bne-designability-card/v0.1.0",
        "card_id" => bytes2hex(sha256(_design_candidate_key(rec)))[1:16],
        "nid" => rec.nid,
        "inp" => rec.inp,
        "out" => rec.out,
        "minimal" => minimal,
        "pass" => pass,
        "tunability_score" => round(score; digits=4),
        "certificate_grade" => grade,
        "screen_status" => pass ? "proxy_pass" : "proxy_near_miss",
        "reason" => pass ? "proxy screen pass; downstream Placer must compute exact parameter placement" :
                           "candidate retained as a certificate or near-miss; inspect active_failures",
        "complexity" => Dict(
            "d" => rec.d, "r" => rec.r, "mu" => rec.mu,
            "n_species" => rec.n_species, "n_complexes" => rec.n_complexes,
            "max_complex_size" => rec.max_complex_size,
            "assembly_depth" => rec.assembly_depth,
            "uses_homomer" => rec.uses_homomer == 1,
            "uses_complex_growth" => rec.uses_complex_growth == 1,
            "uses_higher_order_template" => rec.uses_higher_order_template == 1,
        ),
        "constraints" => Dict(
            "kd_bounds" => [kd_bounds[1], kd_bounds[2]],
            "total_bounds" => [total_bounds[1], total_bounds[2]],
            "min_dynamic_range" => min_dynamic_range,
            "min_transition_spacing_decades" => min_transition_spacing,
            "min_chebyshev_radius" => min_margin_radius,
            "supported_handles" => ["reaction_order_program", "kd_bounds", "atlas_volume", "robust_path_count"],
            "unsupported_handles" => ["finite_dose_peak_uniqueness", "multi_input_surface_exact_chebyshev", "temporal_dynamics"],
            "supported_constraints" => Any[
                Dict("path" => "target", "kind" => "reaction_order_program",
                     "support_level" => "enforced", "stage" => "atlas_match",
                     "solver" => "design_index"),
                Dict("path" => "tuning.kd_bounds", "kind" => "parameter_box",
                     "support_level" => "proxy_only", "stage" => "screen",
                     "solver" => "design_screen_proxy"),
            ],
            "unsupported_constraints" => Any[
                Dict("path" => "temporal_dynamics", "kind" => "dynamics",
                     "support_level" => "ignored", "stage" => "screen",
                     "solver" => "none",
                     "reason" => "design_screen currently ranks equilibrium ROP candidates"),
            ],
            "bounds_intersection_verified" => false,
            "total_bounds_role" => "forwarded_to_downstream_placer_not_used_by_proxy_screen",
        ),
        "metrics" => Dict(
            "chebyshev_radius" => round(cheb_radius; digits=4),
            "chebyshev_radius_source" => "atlas_volume_radius_proxy_not_exact_feasible_region",
            "chebyshev_units" => "log10_qK_euclidean_proxy",
            "tunable_volume" => round(tunable_volume; digits=6),
            "tunable_volume_sum" => round(volume_sum; digits=6),
            "dynamic_range" => round(dynamic_range; digits=4),
            "transition_spacing" => round(transition_spacing; digits=4),
            "parameter_extremeness" => round(parameter_extremeness; digits=4),
            "condition_number" => round(condition_number; digits=4),
            "parameter_breakpoint_sensitivity" => round(sensitivity; digits=4),
            "sampled_robustness" => round(sampled_robustness; digits=4),
            "robust_path_count" => rec.robust_paths,
            "feasible_path_count" => rec.feasible_paths,
            "total_path_count" => rec.total_paths,
            "sl_nswitch" => rec.sl_nswitch,
            "sl_absro" => rec.sl_absro,
        ),
        "parameter_recommendation" => Dict(
            "theta_star" => Dict(
                "status" => "not_computed",
                "source" => "declared_box_center_seed_only",
                "placement_status" => "not_attempted",
                "verification_status" => "not_attempted",
                "source_type" => "seed",
                "source_endpoint" => "/api/design_screen",
                "bounds_verified" => false,
                "log_qK" => log_qK,
                "kd" => kd,
                "totals" => totals,
                "note" => "This is a neutral downstream seed, not a solved parameter recommendation. Exact behavior Chebyshev is computed by Placer when a linear ROP component is selected.",
            ),
            "downstream" => Dict(
                "model_builder" => true,
                "placer" => true,
                "build_and_tune_uses" => "selected recommended candidate",
            ),
        ),
        "placement_attempt" => Dict(
            "attempted" => false,
            "status" => "not_attempted",
            "endpoint" => "/api/design_screen",
            "mode" => "none",
        ),
        "certificate_stack" => Any[
            Dict("grade" => grade,
                 "scope" => "atlas_slice",
                 "soundness" => grade == "atlas-volume-proxy" ? "proxy" : "surrogate",
                 "source" => "design_index",
                 "supports_exact_placement" => false),
        ],
        "active_failures" => active_failures,
        "active_failure_details" => Any[
            Dict("code" => failure,
                 "severity" => "warning",
                 "stage" => "proxy_screen",
                 "blocks_recommendation" => true,
                 "message_agent" => failure)
            for failure in active_failures
        ],
        "agent_handoff" => Dict(
            "endpoint" => "/api/design_screen",
            "role" => "candidate_screen_result",
            "next_actions" => ["select_network", "build_model", "run_parameter_placer"],
            "agent_should_not" => ["invent_curve", "treat_proxy_radius_as_exact_linear_certificate"],
        ),
    )
end

function _design_card_sort_key(card)
    metrics = card["metrics"]
    complexity = card["complexity"]
    return (-Float64(card["tunability_score"]),
            -Float64(metrics["chebyshev_radius"]),
            -Float64(metrics["tunable_volume"]),
            Int(complexity["r"]),
            Int(complexity["d"]),
            Int(complexity["mu"]))
end

function _design_ranking_preferences(ranking_policy)
    supported = Set(["certificate_grade", "chebyshev_radius", "tunable_volume",
                     "dynamic_range", "transition_spacing", "sampled_robustness",
                     "complexity", "condition_number"])
    raw = collect(_raw_get(ranking_policy, :prefer,
        Any["certificate_grade", "chebyshev_radius", "tunable_volume", "complexity"]))
    prefs = [String(p) for p in raw if String(p) in supported]
    return isempty(prefs) ? ["certificate_grade", "chebyshev_radius", "tunable_volume", "complexity"] : prefs
end

function _design_certificate_rank(card)
    grade = String(get(card, "certificate_grade", ""))
    grade == "exact-linear" && return 3.0
    grade == "union-of-polytopes" && return 2.0
    return card["pass"] === true ? 1.0 : 0.0
end

function _design_object_keys(obj)
    obj === nothing && return String[]
    try
        return [String(k) for k in keys(obj)]
    catch
        return String[]
    end
end

function _design_spec_interpretation(target_kind::AbstractString, target, designability_spec;
                                     exact_placement=Dict{String, Any}())
    supported = Set(["target_kind", "target", "candidate_budget", "tuning", "ranking_policy"])
    raw_keys = _design_object_keys(designability_spec)
    unhandled = [key for key in raw_keys if !(key in supported)]
    attempted = Int(_raw_get(exact_placement, :attempted, 0))
    succeeded = Int(_raw_get(exact_placement, :succeeded, 0))
    scope = if attempted > 0
        "atlas reaction-order match plus budgeted exact single-RO placement for attempted candidates; multi-regime and temporal specs remain downstream"
    else
        "atlas reaction-order match plus tunability proxy ranking; exact parameter placement remains downstream unless candidate_budget enables exact placement for a supported target"
    end
    return Dict(
        "normalized_target_kind" => String(target_kind),
        "normalized_target" => target,
        "supported_clauses" => ["target_kind", "target", "candidate_budget", "tuning", "ranking_policy"],
        "unhandled_clauses" => sort(unhandled),
        "degradation" => isempty(unhandled) ?
            "none" :
            "unsupported clauses were preserved in designability_spec but not enforced by the active screen",
        "current_solver_scope" => scope,
        "exact_placement" => Dict(
            "attempted" => attempted,
            "succeeded" => succeeded,
            "scope" => "single_ro",
        ),
    )
end

function _design_card_preference_value(card, pref::AbstractString)
    metrics = card["metrics"]
    complexity = card["complexity"]
    pref == "certificate_grade" && return _design_certificate_rank(card)
    pref == "chebyshev_radius" && return Float64(metrics["chebyshev_radius"])
    pref == "tunable_volume" && return Float64(metrics["tunable_volume"])
    pref == "dynamic_range" && return Float64(metrics["dynamic_range"])
    pref == "transition_spacing" && return Float64(metrics["transition_spacing"])
    pref == "sampled_robustness" && return Float64(metrics["sampled_robustness"])
    pref == "condition_number" && return -Float64(metrics["condition_number"])
    if pref == "complexity"
        return -(Float64(complexity["r"]) + Float64(complexity["d"]) + Float64(complexity["mu"]))
    end
    return 0.0
end

function _design_exact_single_ro(target_kind::AbstractString, target)
    target_kind == "exact" || return nothing
    vals = collect(target)
    length(vals) == 1 || return nothing
    return Float64(first(vals))
end

function _design_string_totals(totals)
    return Dict(String(k) => Float64(v) for (k, v) in pairs(totals))
end

function _design_exact_placement_budget(candidate_budget)
    nested = _design_dict_or_empty(_raw_get(candidate_budget, :exact_placement_budget, Dict{String, Any}()))
    nested_max = _raw_get(nested, :max_exact_placements, _raw_get(nested, :max_candidates, nothing))
    raw_max = _raw_get(candidate_budget, :max_exact_placements,
        nested_max === nothing ? 0 : nested_max)
    max_exact = max(0, Int(raw_max))
    return Dict(
        "max_exact_placements" => max_exact,
        "selection_policy" => String(_raw_get(nested, :selection_policy, "ranked_proxy_prefix")),
        "per_candidate_modes" => String.(collect(_raw_get(nested, :per_candidate_modes, Any["single_ro"]))),
        "requested" => max_exact,
    )
end

function _design_record_exact_failure!(card, code::AbstractString, reason::AbstractString;
                                       stage::AbstractString="exact_placement")
    push!(card["active_failures"], String(code))
    push!(card["active_failure_details"], Dict(
        "code" => String(code),
        "severity" => "warning",
        "stage" => String(stage),
        "blocks_recommendation" => true,
        "message_agent" => String(reason),
    ))
    return card
end

function _design_try_exact_linear_card(card, rec, target_ro::Real, tuning)
    kd_bounds = _design_bounds(tuning, :kd_bounds, (-3.0, 3.0))
    tol = _design_float(tuning, :exact_ro_tol, 0.05)
    verify_h = _design_float(tuning, :exact_verify_h, 0.02)
    updated = deepcopy(card)
    try
        placement = placer_place_bounded(_design_nid_to_rules(rec.nid), rec.inp, rec.out,
            Float64(target_ro); tol=tol, verify_h=verify_h, kd_bounds=kd_bounds)
        updated["placement_attempt"] = Dict(
            "attempted" => true,
            "status" => placement.feasible ? "unverified" : "infeasible",
            "endpoint" => "/api/design_screen",
            "mode" => "single_ro",
            "target_ro" => Float64(target_ro),
            "kd_bounds_used" => [kd_bounds[1], kd_bounds[2]],
            "pass" => getproperty(placement, :feasible) ? getproperty(placement, :pass) : false,
        )
        if placement.feasible && placement.pass
            cheb = Float64(placement.chebyshev_radius)
            isfinite(cheb) || (cheb = 0.0)
            metrics = updated["metrics"]
            metrics["chebyshev_radius"] = round(cheb; digits=4)
            metrics["chebyshev_radius_source"] = "placer_place_bounded"
            metrics["chebyshev_units"] = "log10_qK_euclidean"
            metrics["predicted_RO"] = round(Float64(placement.predicted_RO); digits=4)
            metrics["measured_RO"] = round(Float64(placement.measured_RO); digits=4)
            metrics["exact_vertex_idx"] = Int(placement.vertex_idx)
            metrics["exact_solve_mode"] = String(placement.solve_mode)

            constraints = updated["constraints"]
            constraints["bounds_intersection_verified"] = true
            constraints["exact_ro_tol"] = tol
            constraints["exact_verify_h"] = verify_h
            constraints["supported_handles"] = sort(collect(Set(vcat(
                String.(constraints["supported_handles"]),
                ["reaction_order_single_slope", "exact_linear_cell_chebyshev"]))))
            push!(constraints["supported_constraints"], Dict(
                "path" => "target",
                "kind" => "reaction_order_single_slope",
                "requested" => Float64(target_ro),
                "effective" => Float64(placement.predicted_RO),
                "support_level" => "enforced",
                "stage" => "exact_placement",
                "solver" => "placer_place_bounded",
                "reason" => "single exact RO target admits a linear ROP cell certificate",
            ))

            theta = updated["parameter_recommendation"]["theta_star"]
            theta["status"] = "computed"
            theta["source"] = "placer_place_bounded"
            theta["placement_status"] = "success"
            theta["verification_status"] = "verified"
            theta["source_type"] = "exact_solver"
            theta["source_endpoint"] = "/api/design_screen"
            theta["bounds_verified"] = true
            theta["solver_version"] = "placer_place_bounded/v1"
            theta["log_qK_basis"] = "log10_qK"
            theta["log_qK"] = Float64.(collect(placement.log_qK))
            theta["kd"] = Float64.(collect(placement.kd))
            theta["totals"] = _design_string_totals(placement.totals)
            theta["note"] = "Exact-linear single-RO placement: Chebyshev/interior point of a target regime intersected with the declared Kd box."

            updated["certificate_grade"] = "exact-linear"
            updated["screen_status"] = "exact_linear_pass"
            updated["pass"] = true
            updated["reason"] = "exact single-RO placement succeeded within declared Kd bounds"
            updated["placement_attempt"]["status"] = "success"
            updated["placement_attempt"]["chebyshev_radius"] = cheb
            updated["placement_attempt"]["measured_RO"] = Float64(placement.measured_RO)
            updated["certificate_stack"] = vcat(Any[
                Dict("grade" => "exact-linear",
                     "scope" => "single_ro_cell",
                     "soundness" => "exact_linear_cell",
                     "source" => "placer_place_bounded",
                     "supports_exact_placement" => true),
            ], updated["certificate_stack"])
            updated["tunability_score"] = max(Float64(updated["tunability_score"]),
                clamp(0.70 + 0.20 * min(cheb / max(kd_bounds[2] - kd_bounds[1], eps(Float64)), 1.0), 0.0, 1.0))
            return updated
        end
        reason = if placement.feasible
            "Exact-linear placement found a regime but forward verification failed: " *
            "target_RO=$(Float64(target_ro)), predicted_RO=$(placement.predicted_RO), measured_RO=$(placement.measured_RO)."
        else
            String(placement.reason)
        end
        code = placement.feasible ? "exact_placement_verification_failed" : "exact_placement_failed"
        _design_record_exact_failure!(updated, code, reason)
        updated["screen_status"] = "proxy_near_miss"
        updated["pass"] = false
        updated["reason"] = reason
        updated["placement_attempt"]["status"] = placement.feasible ? "unverified" : "infeasible"
        updated["placement_attempt"]["error_code"] = code
        updated["placement_attempt"]["reason"] = reason
        if placement.feasible
            updated["placement_attempt"]["measured_RO"] = Float64(placement.measured_RO)
            updated["placement_attempt"]["predicted_RO"] = Float64(placement.predicted_RO)
        end
        updated["parameter_recommendation"]["theta_star"]["note"] =
            "Exact-linear placement was attempted but did not produce a verified point; inspect active_failures and reason."
        return updated
    catch e
        e isa SyncBudgetExceeded && rethrow()
        reason = "exact placement error: $(sprint(showerror, e))"
        _design_record_exact_failure!(updated, "exact_placement_error", reason)
        updated["screen_status"] = "proxy_near_miss"
        updated["pass"] = false
        updated["reason"] = reason
        updated["placement_attempt"] = Dict(
            "attempted" => true,
            "status" => "error",
            "endpoint" => "/api/design_screen",
            "mode" => "single_ro",
            "target_ro" => Float64(target_ro),
            "kd_bounds_used" => [kd_bounds[1], kd_bounds[2]],
            "pass" => false,
            "error_code" => "exact_placement_error",
            "reason" => reason,
        )
        updated["parameter_recommendation"]["theta_star"]["note"] =
            "Exact-linear placement errored; proxy metrics remain available but this card is not a verified recommendation."
        return updated
    end
end

function _design_card_less(a, b, preferences)
    for pref in preferences
        av = _design_card_preference_value(a, pref)
        bv = _design_card_preference_value(b, pref)
        av == bv && continue
        return av > bv
    end
    return _design_card_sort_key(a) < _design_card_sort_key(b)
end

function design_screen(target_kind::AbstractString, target;
                       candidate_budget=Dict{String, Any}(),
                       tuning=Dict{String, Any}(),
                       ranking_policy=Dict{String, Any}(),
                       designability_spec=nothing)
    spec = if designability_spec isa AbstractDict &&
              String(_raw_get(designability_spec, :schema_version, "")) == DESIGNABILITY_SPEC_VERSION
        Dict{String, Any}(_materialize(designability_spec))
    else
        base = legacy_designability_spec(target_kind, target;
            candidate_budget = candidate_budget,
            tuning = tuning,
            ranking_policy = ranking_policy)
        if designability_spec isa AbstractDict
            for (key, value) in pairs(_materialize(designability_spec))
                skey = String(key)
                skey in ("target_kind", "target") && continue
                base["target"][skey] = value
            end
        end
        base
    end
    return design_screen_from_spec(spec)
end

# POST /api/design_search — { target_kind:"sign"|"exact", target:"+-+" | [1.5,0,-1] }
function handle_design_search(req)
    body = read_json(req)
    res = try
        kind, target = _design_normalize_design_target(
            _raw_get(body, :target_kind, "sign"),
            _raw_get(body, :target, nothing))
        design_search(kind, target)
    catch e
        return error_response("design_search failed: $(sprint(showerror, e))"; status = 400)
    end
    return json_response(res)
end

# POST /api/design_screen — tunability-aware wrapper around design_search.
function handle_design_screen(req)
    body = read_json(req)
    res = try
        schema_version = _raw_get(body, :schema_version, nothing)
        spec = if schema_version isa AbstractString &&
                  String(schema_version) == DESIGNABILITY_SPEC_VERSION
            Dict{String, Any}(_materialize(body))
        elseif _raw_haskey(body, :designability_spec)
            _raw_get(body, :designability_spec, nothing)
        else
            base = legacy_designability_spec(
                _raw_get(body, :target_kind, "sign"),
                _raw_get(body, :target, nothing);
                candidate_budget = _raw_get(body, :candidate_budget, Dict{String, Any}()),
                tuning = _raw_get(body, :tuning, Dict{String, Any}()),
                ranking_policy = _raw_get(body, :ranking_policy, Dict{String, Any}()),
            )
            for (key, value) in pairs(_materialize(body))
                skey = String(key)
                skey in ("target_kind", "target", "candidate_budget", "tuning", "ranking_policy") && continue
                base["target"][skey] = value
            end
            base
        end
        design_screen_from_spec(spec)
    catch e
        e isa SyncBudgetExceeded && rethrow()
        return error_response("design_screen failed: $(sprint(showerror, e))"; status = 400)
    end
    return json_response(res)
end

# GET/POST /api/design_labels — the behavior base-motif vocab (16 labels) with a
# representative RO program each, so the UI can show "label → RO language". Sourced
# from the tracked data products (motif_rep_prog.json + periodic_table.json
# vocab.base_motifs) so the picker never drifts from the atlas.
function handle_design_labels(req)
    dir = normpath(joinpath(@__DIR__, "..", "..", "paper_rop_periodic_table", "data"))
    rep_path   = joinpath(dir, "motif_rep_prog.json")
    vocab_path = joinpath(dir, "periodic_table.json")
    isfile(rep_path)   || return error_response("motif_rep_prog.json not found at $rep_path"; status = 500)
    isfile(vocab_path) || return error_response("periodic_table.json not found at $vocab_path"; status = 500)
    rep = JSON3.read(read(rep_path, String))            # label -> "1 → 0 → -1"
    pt  = JSON3.read(read(vocab_path, String))
    counts = Dict{String, Int}()
    vm = get(pt, :vocab, nothing)
    if vm !== nothing
        bm = get(vm, :base_motifs, nothing)
        if bm !== nothing
            for (k, v) in pairs(bm); counts[String(k)] = Int(v); end
        end
    end
    labels = [Dict("label" => String(k),
                    "ro_program" => String(rep[k]),
                    "count" => get(counts, String(k), 0))
              for k in sort(collect(keys(rep)))]
    return json_response(Dict("labels" => labels))
end
