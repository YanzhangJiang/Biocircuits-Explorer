# Best-effort NUMERIC level solve (the quantitative layer, NOT a clean regime solve):
# adjust one total so the output reaches a target level at a given operating input.
function placer_level(rules::Vector{String}, kd::Vector{Float64}, totals::Dict{Symbol, Float64},
                      input_sym, output_sym, operating_input::Real, target_level::Real, adjust_sym)
    input_sym = Symbol(input_sym); output_sym = Symbol(output_sym); adjust_sym = Symbol(adjust_sym)
    model, _, _, _ = build_model(rules, kd)
    input_idx  = locate_sym_qK(model, input_sym);  input_idx  === nothing && error("unknown input $input_sym")
    adjust_idx = locate_sym_qK(model, adjust_sym); adjust_idx === nothing && error("unknown adjust total $adjust_sym")
    output_idx = locate_sym_x(model, output_sym);  output_idx === nothing && error("unknown output $output_sym")
    qsyms = Symbol.(string.(q_sym(model)))
    base = zeros(Float64, model.n)
    for i in 1:model.d; base[i] = log10(get(totals, qsyms[i], 1.0)); end
    for j in 1:model.r; base[model.d + j] = log10(kd[j]); end
    base[input_idx] = log10(Float64(operating_input))
    out_at(la) = begin
        p = copy(base); p[adjust_idx] = la
        qK2x(model, p; input_logspace = true, output_logspace = true)[output_idx]   # log10[output]
    end
    tgt = log10(max(Float64(target_level), 1e-300))
    lo, hi = -4.0, 8.0
    flo = out_at(lo) - tgt; fhi = out_at(hi) - tgt
    local la::Float64; feasible = true
    if flo * fhi > 0
        la = abs(flo) < abs(fhi) ? lo : hi; feasible = false
    else
        for _ in 1:60
            mid = (lo + hi) / 2; fm = out_at(mid) - tgt
            if flo * fm <= 0; hi = mid; else; lo = mid; flo = fm; end
        end
        la = (lo + hi) / 2
    end
    nt = copy(totals); nt[adjust_sym] = exp10(la)
    return (; feasible, adjusted_total = adjust_sym, adjusted_value = exp10(la),
            target_level = Float64(target_level), achieved_level = exp10(out_at(la)),
            kd = collect(Float64.(kd)), totals = nt)
end
# POST /api/placer_level — { rules|session, input_sym, output_sym, kd, totals, operating_input, target_level, adjust_sym }
function handle_placer_level(req)
    body = read_json(req)
    rules = if _raw_haskey(body, :rules)
        String.(collect(_raw_get(body, :rules, String[])))
    else
        bundle, err = _resolve_bundle_or_response(body); err === nothing || return err
        String.(collect(bundle["rules"]))
    end
    isempty(rules) && return error_response("No reactions"; status = 400)
    for k in (:input_sym, :output_sym, :operating_input, :target_level, :adjust_sym, :kd)
        _raw_haskey(body, k) || return error_response("`$k` is required"; status = 400)
    end
    input_sym  = Symbol(_raw_get(body, :input_sym, ""))
    output_sym = Symbol(_raw_get(body, :output_sym, ""))
    kd = Float64.(collect(_raw_get(body, :kd, Float64[])))
    totals = Dict{Symbol, Float64}()
    tw = _raw_get(body, :totals, nothing)
    tw === nothing || for (k, v) in pairs(tw); totals[Symbol(k)] = Float64(v); end
    res = try
        placer_level(rules, kd, totals, input_sym, output_sym,
                     Float64(_raw_get(body, :operating_input, 1.0)),
                     Float64(_raw_get(body, :target_level, 1.0)),
                     Symbol(_raw_get(body, :adjust_sym, "")))
    catch e
        return error_response("Level solve failed: $(sprint(showerror, e))"; status = 400)
    end
    kd_vec = collect(Float64.(res.kd))
    curve = try
        placer_dose_response(rules, kd_vec, res.totals, input_sym, String(output_sym))
    catch e
        return error_response("Dose-response curve failed: $(sprint(showerror, e))"; status = 400)
    end
    return json_response(Dict(
        "adjusted_total" => string(res.adjusted_total), "adjusted_value" => res.adjusted_value,
        "target_level" => res.target_level, "achieved_level" => json_safe_real(res.achieved_level),
        "feasible" => res.feasible, "totals" => Dict(string(k) => v for (k, v) in res.totals),
        "dose_response_curve" => curve,
    ))
end

