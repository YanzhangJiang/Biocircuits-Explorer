#!/usr/bin/env julia

using Dates
using JSON3
using LinearAlgebra
using BiocircuitsExplorerBackend

const REPO_ROOT = abspath(joinpath(@__DIR__, "..", ".."))

function parse_args(args)
    parsed = Dict{String, String}()
    idx = 1
    while idx <= length(args)
        key = args[idx]
        if key in ("--run-id", "--d", "--mu", "--candidates-json", "--output-json", "--network-parallelism")
            idx == length(args) && error("Missing value for $(key)")
            parsed[key[3:end]] = args[idx + 1]
            idx += 2
        else
            error("Unknown argument: $(key)")
        end
    end
    for required in ("run-id", "d", "mu", "candidates-json", "output-json")
        haskey(parsed, required) || error("Missing --$(required)")
    end
    return parsed
end

utc_now() = Dates.format(now(Dates.UTC), dateformat"yyyy-mm-ddTHH:MM:SSZ")

function read_json(path)
    return JSON3.read(read(path, String), Dict{String, Any})
end

function write_json(path, payload)
    mkpath(dirname(path))
    tmp = path * ".tmp"
    open(tmp, "w") do io
        JSON3.write(io, payload)
        write(io, "\n")
    end
    mv(tmp, path; force=true)
end

function as_string_vector(value)
    value === nothing && return String[]
    return [String(item) for item in collect(value)]
end

function raw_get(raw, key::String, default=nothing)
    if haskey(raw, key)
        return raw[key]
    elseif haskey(raw, Symbol(key))
        return raw[Symbol(key)]
    end
    return default
end

function finite_number(value)
    if value isa Real
        x = Float64(value)
    else
        text = lowercase(strip(String(value)))
        if text in ("nan", "+nan", "-nan")
            return nothing, "nan"
        elseif text in ("inf", "+inf", "infinity", "+infinity")
            return nothing, "pos_inf"
        elseif text in ("-inf", "-infinity")
            return nothing, "neg_inf"
        else
            x = tryparse(Float64, text)
            x === nothing && return nothing, "unknown"
        end
    end
    isnan(x) && return nothing, "nan"
    isinf(x) && return nothing, signbit(x) ? "neg_inf" : "pos_inf"
    return x, "none"
end

function sign3(value; eps=1e-9)
    x, singular = finite_number(value)
    x === nothing && return nothing, singular
    if x > eps
        return 1, "none"
    elseif x < -eps
        return -1, "none"
    else
        return 0, "none"
    end
end

function rle_keep_zero(signs)
    out = Int[]
    for sign in signs
        if isempty(out) || out[end] != sign
            push!(out, sign)
        end
    end
    return out
end

format_sign(sign::Integer) = sign == 1 ? "+" : (sign == -1 ? "-" : "0")

function scalar_profile(profile)
    profile isa AbstractVector || return nothing
    values = collect(profile)
    isempty(values) && return nothing
    any(item -> item isa AbstractVector, values) && return nothing
    return values
end

function sign_program_summary(profile)
    values = scalar_profile(profile)
    values === nothing && return nothing
    signs = Int[]
    singular = Any[]
    for (idx, value) in enumerate(values)
        sign, singular_kind = sign3(value)
        if sign === nothing
            push!(singular, Dict("index" => idx, "singular" => singular_kind))
        else
            push!(signs, sign)
        end
    end
    rle = rle_keep_zero(signs)
    transitions = String[string(format_sign(left), "->", format_sign(right)) for (left, right) in zip(rle[1:end-1], rle[2:end])]
    transition_pairs = collect(zip(rle[1:end-1], rle[2:end]))
    via_zero = any(i -> rle[i:i+2] == [1, 0, -1] || rle[i:i+2] == [-1, 0, 1], 1:max(0, length(rle) - 2))
    direct = any(pair -> pair == (1, -1) || pair == (-1, 1), transition_pairs)
    settle = length(rle) > 1 && rle[end] == 0 && any(sign -> sign != 0, rle[1:end-1])
    return Dict(
        "eps" => 1e-9,
        "program" => values,
        "program_signs" => signs,
        "program_sign_rle" => rle,
        "singular" => singular,
        "sign_state_set" => sort!(unique(format_sign.(rle))),
        "sign_transitions" => transitions,
        "three_state" => Set(rle) == Set([-1, 0, 1]),
        "opposite_sign_program" => (1 in rle) && (-1 in rle),
        "via_zero_opposite_switch" => via_zero,
        "direct_opposite_switch" => direct,
        "settle_to_zero" => settle,
        "max_sign_switch_count" => max(0, length(rle) - 1),
    )
end

function ultrasensitivity_summary(profile, mu::Integer)
    values = scalar_profile(profile)
    values === nothing && return nothing
    finite = Float64[]
    singular = Any[]
    for (idx, value) in enumerate(values)
        x, singular_kind = finite_number(value)
        if x === nothing
            singular_kind == "none" || push!(singular, Dict("index" => idx, "singular" => singular_kind))
        else
            push!(finite, x)
        end
    end
    return Dict(
        "definition" => "finite_RO_greater_than_mu",
        "mu" => Int(mu),
        "max_finite_ro" => isempty(finite) ? nothing : maximum(finite),
        "max_abs_finite_ro" => isempty(finite) ? nothing : maximum(abs.(finite)),
        "singular" => singular,
    )
end

function witness_payload(property_id, entry, slice, family, summary, strength)
    metadata = raw_get(entry, "source_metadata", Dict{String, Any}())
    return Dict(
        "property_id" => property_id,
        "network_id" => String(raw_get(entry, "network_id", "")),
        "source_label" => String(raw_get(entry, "source_label", "")),
        "network_reactions" => collect(raw_get(entry, "raw_rules", Any[])),
        "network_canonical" => String(raw_get(entry, "canonical_code", raw_get(entry, "network_id", ""))),
        "source_metadata" => metadata,
        "input_slice" => Dict(
            "slice_id" => String(raw_get(slice, "slice_id", "")),
            "input_symbol" => raw_get(slice, "input_symbol", nothing),
            "change_signature" => raw_get(slice, "change_signature", nothing),
            "change_qk_symbols" => collect(raw_get(slice, "change_qk_symbols", Any[])),
            "change_qk_signs" => collect(raw_get(slice, "change_qk_signs", Any[])),
        ),
        "output_symbol" => String(raw_get(slice, "output_symbol", "")),
        "program" => raw_get(family, "exact_profile", Any[]),
        "program_label" => raw_get(family, "family_label", nothing),
        "path_count" => raw_get(family, "path_count", nothing),
        "representative_path_length" => raw_get(family, "representative_path_length", nothing),
        "program_summary" => summary,
        "strength" => strength,
        "backend" => "BiocircuitsExplorerBackend.build_behavior_atlas",
    )
end

function better_hit(candidate, current)
    current === nothing && return true
    cmeta = raw_get(candidate, "source_metadata", Dict{String, Any}())
    bmeta = raw_get(current, "source_metadata", Dict{String, Any}())
    cfeat = raw_get(cmeta, "features", Dict{String, Any}())
    bfeat = raw_get(bmeta, "features", Dict{String, Any}())
    ckey = (
        Int(raw_get(cfeat, "reaction_count", 999999)),
        Int(raw_get(cfeat, "assembly_depth", 999999)),
        Int(raw_get(cfeat, "complex_count", 999999)),
        length(String(raw_get(candidate, "program_label", ""))),
        String(raw_get(candidate, "network_canonical", "")),
    )
    bkey = (
        Int(raw_get(bfeat, "reaction_count", 999999)),
        Int(raw_get(bfeat, "assembly_depth", 999999)),
        Int(raw_get(bfeat, "complex_count", 999999)),
        length(String(raw_get(current, "program_label", ""))),
        String(raw_get(current, "network_canonical", "")),
    )
    return ckey < bkey
end

function collect_scalar_hits!(hits, atlas, mu::Integer)
    entries = Dict(String(raw_get(entry, "network_id", "")) => entry for entry in collect(raw_get(atlas, "network_entries", Any[])))
    slices = Dict(String(raw_get(slice, "slice_id", "")) => slice for slice in collect(raw_get(atlas, "behavior_slices", Any[])))
    for family in collect(raw_get(atlas, "family_buckets", Any[]))
        String(raw_get(family, "family_kind", "")) == "exact" || continue
        slice_id = String(raw_get(family, "slice_id", ""))
        haskey(slices, slice_id) || continue
        slice = slices[slice_id]
        entry = get(entries, String(raw_get(slice, "network_id", "")), nothing)
        entry === nothing && continue
        profile = raw_get(family, "exact_profile", Any[])
        summary = sign_program_summary(profile)
        summary === nothing && continue

        if Int(raw_get(summary, "max_sign_switch_count", 0)) > 0
            payload = witness_payload("sign_switch.v1", entry, slice, family, summary, summary)
            better_hit(payload, get(hits, "sign_switch.v1", nothing)) && (hits["sign_switch.v1"] = payload)
        end
        if Bool(raw_get(summary, "settle_to_zero", false))
            payload = witness_payload("settle_to_zero.v1", entry, slice, family, summary, summary)
            better_hit(payload, get(hits, "settle_to_zero.v1", nothing)) && (hits["settle_to_zero.v1"] = payload)
        end

        us = ultrasensitivity_summary(profile, mu)
        if us !== nothing && raw_get(us, "max_finite_ro", nothing) !== nothing && Float64(raw_get(us, "max_finite_ro", -Inf)) > mu
            payload = witness_payload("ultrasensitivity.v1", entry, slice, family, Dict("program" => scalar_profile(profile)), us)
            better_hit(payload, get(hits, "ultrasensitivity.v1", nothing)) && (hits["ultrasensitivity.v1"] = payload)
        end
    end
end

function sign_matrix(matrix; eps=1e-9)
    pattern = Any[]
    singular = Any[]
    for row_idx in 1:size(matrix, 1)
        row = Int[]
        for col_idx in 1:size(matrix, 2)
            x = matrix[row_idx, col_idx]
            if isnan(x) || isinf(x)
                push!(row, 0)
                push!(singular, Dict("row" => row_idx, "col" => col_idx, "value" => x))
            elseif x > eps
                push!(row, 1)
            elseif x < -eps
                push!(row, -1)
            else
                push!(row, 0)
            end
        end
        push!(pattern, row)
    end
    return Dict("pattern" => pattern, "singular" => singular)
end

function collect_mimo_hits!(hits, atlas)
    entries = Dict(String(raw_get(entry, "network_id", "")) => entry for entry in collect(raw_get(atlas, "network_entries", Any[])))
    grouped = Dict{Tuple{String, Int}, Vector{Any}}()
    for rec in collect(raw_get(atlas, "regime_records", Any[]))
        qk = collect(raw_get(rec, "change_qk_symbols", Any[]))
        length(qk) >= 2 || continue
        value = raw_get(rec, "output_order_value", nothing)
        value isa AbstractVector || continue
        key = (String(raw_get(rec, "graph_slice_id", "")), Int(raw_get(rec, "vertex_idx", 0)))
        push!(get!(grouped, key, Any[]), rec)
    end
    for ((graph_slice_id, vertex_idx), records) in grouped
        length(records) >= 2 || continue
        sort!(records; by=rec -> String(raw_get(rec, "output_symbol", "")))
        qk_symbols = as_string_vector(raw_get(first(records), "change_qk_symbols", Any[]))
        matrix = fill(NaN, length(records), length(qk_symbols))
        finite_ok = true
        for (row_idx, rec) in enumerate(records)
            values = collect(raw_get(rec, "output_order_value", Any[]))
            length(values) == length(qk_symbols) || (finite_ok = false; break)
            for col_idx in eachindex(values)
                x, singular = finite_number(values[col_idx])
                if x === nothing
                    finite_ok = false
                    matrix[row_idx, col_idx] = NaN
                else
                    matrix[row_idx, col_idx] = x
                end
            end
        end
        finite_ok || continue
        matrix_rank = rank(matrix)
        matrix_rank >= 2 || continue

        first_rec = first(records)
        entry = get(entries, String(raw_get(first_rec, "network_id", "")), nothing)
        entry === nothing && continue
        strength = Dict(
            "max_rank_G" => matrix_rank,
            "matrix" => [collect(matrix[row_idx, :]) for row_idx in 1:size(matrix, 1)],
            "sign_pattern" => sign_matrix(matrix),
            "input_symbols" => qk_symbols,
            "output_symbols" => [String(raw_get(rec, "output_symbol", "")) for rec in records],
            "vertex_idx" => vertex_idx,
        )
        payload = Dict(
            "property_id" => "mimo_gain.v1",
            "network_id" => String(raw_get(entry, "network_id", "")),
            "source_label" => String(raw_get(entry, "source_label", "")),
            "network_reactions" => collect(raw_get(entry, "raw_rules", Any[])),
            "network_canonical" => String(raw_get(entry, "canonical_code", raw_get(entry, "network_id", ""))),
            "source_metadata" => raw_get(entry, "source_metadata", Dict{String, Any}()),
            "input_slice" => Dict(
                "graph_slice_id" => graph_slice_id,
                "change_signature" => raw_get(first_rec, "change_signature", nothing),
                "change_qk_symbols" => qk_symbols,
                "change_qk_signs" => collect(raw_get(first_rec, "change_qk_signs", Any[])),
            ),
            "output_symbol" => join(strength["output_symbols"], ","),
            "program" => strength["matrix"],
            "program_label" => "rank_$(matrix_rank)_mimo_gain",
            "program_summary" => strength,
            "strength" => strength,
            "backend" => "BiocircuitsExplorerBackend.regime_records",
        )
        better_hit(payload, get(hits, "mimo_gain.v1", nothing)) && (hits["mimo_gain.v1"] = payload)
    end
end

function main(args)
    parsed = parse_args(args)
    run_id = parsed["run-id"]
    d = parse(Int, parsed["d"])
    mu = parse(Int, parsed["mu"])
    network_parallelism = parse(Int, get(parsed, "network-parallelism", "1"))
    payload = read_json(parsed["candidates-json"])
    candidates = collect(raw_get(payload, "candidates", Any[]))

    behavior_config = AtlasBehaviorConfig(
        path_scope=:feasible,
        min_volume_mean=0.0,
        include_path_records=false,
        compute_volume=false,
        keep_singular=true,
    )
    search_profile = AtlasSearchProfile(
        name="periodic_d_mu_v0_backend_projection",
        max_base_species=d,
        max_reactions=max(1, maximum([length(collect(raw_get(candidate, "reactions", Any[]))) for candidate in candidates]; init=1)),
        max_support=mu,
        allow_homomeric_templates=true,
        max_homomer_order=2,
        slice_mode=:change,
        input_mode=:totals_only,
    )
    change_expansion = AtlasChangeExpansionSpec(
        mode=:axes_only,
        max_active_dims=1,
        include_axis_slices=true,
        include_negative_directions=false,
    )

    out = Dict{String, Any}(
        "time_utc" => utc_now(),
        "run_id" => run_id,
        "d" => d,
        "mu" => mu,
        "candidate_count" => length(candidates),
        "status" => "started",
        "hits" => Any[],
        "errors" => Any[],
    )

    try
        atlas = build_behavior_atlas(
            candidates;
            search_profile=search_profile,
            behavior_config=behavior_config,
            change_expansion=change_expansion,
            network_parallelism=network_parallelism,
        )
        length(collect(raw_get(atlas, "path_records", Any[]))) == 0 || error("property batch persisted path records unexpectedly")

        hits = Dict{String, Any}()
        collect_scalar_hits!(hits, atlas, mu)
        collect_mimo_hits!(hits, atlas)
        out["status"] = "ok"
        out["hits"] = collect(values(hits))
        out["counts"] = Dict(
            "input_network_count" => raw_get(atlas, "input_network_count", 0),
            "unique_network_count" => raw_get(atlas, "unique_network_count", 0),
            "successful_network_count" => raw_get(atlas, "successful_network_count", 0),
            "failed_network_count" => raw_get(atlas, "failed_network_count", 0),
            "excluded_network_count" => raw_get(atlas, "excluded_network_count", 0),
            "behavior_slice_count" => length(collect(raw_get(atlas, "behavior_slices", Any[]))),
            "regime_record_count" => length(collect(raw_get(atlas, "regime_records", Any[]))),
            "transition_record_count" => length(collect(raw_get(atlas, "transition_records", Any[]))),
            "family_bucket_count" => length(collect(raw_get(atlas, "family_buckets", Any[]))),
            "path_record_count" => length(collect(raw_get(atlas, "path_records", Any[]))),
        )
    catch err
        out["status"] = "error"
        push!(out["errors"], Dict(
            "error_type" => string(typeof(err)),
            "message" => sprint(showerror, err, catch_backtrace()),
        ))
    end
    out["completed_at_utc"] = utc_now()
    write_json(parsed["output-json"], out)
    println(JSON3.write(Dict(
        "status" => out["status"],
        "candidate_count" => out["candidate_count"],
        "hit_count" => length(out["hits"]),
        "output_json" => parsed["output-json"],
    )))
    return out["status"] == "ok" ? 0 : 1
end

exit(main(ARGS))
