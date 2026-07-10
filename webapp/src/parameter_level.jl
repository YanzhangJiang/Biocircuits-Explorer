# Best-effort NUMERIC level solve (the quantitative layer, NOT a clean regime solve):
# adjust one total so the output reaches a target level at a given operating input.
function placer_level(rules::Vector{String}, kd::Vector{Float64}, totals::Dict{Symbol, Float64},
                      input_sym, output_sym, operating_input::Real, target_level::Real, adjust_sym)
    enforce_sync_rule_budget(rules)
    length(kd) == length(rules) || error("Length of kd must match reactions")
    all(x -> isfinite(x) && x > 0, kd) || error("All Kd values must be finite and positive")
    operating = _placer_finite_number(
        operating_input, "operating_input"; positive=true)
    target = _placer_finite_number(target_level, "target_level"; positive=true)
    isfinite(operating) && operating > 0 || error("operating_input must be finite and positive")
    isfinite(target) && target > 0 || error("target_level must be finite and positive")
    input_sym = Symbol(input_sym); output_sym = Symbol(output_sym); adjust_sym = Symbol(adjust_sym)
    model, _, _, _ = build_model(rules, kd)
    enforce_sync_model_budget(model)
    input_idx  = locate_sym_qK(model, input_sym);  input_idx  === nothing && error("unknown input $input_sym")
    adjust_idx = locate_sym_qK(model, adjust_sym); adjust_idx === nothing && error("unknown adjust total $adjust_sym")
    output_idx = locate_sym_x(model, output_sym);  output_idx === nothing && error("unknown output $output_sym")
    qsyms = Symbol.(string.(q_sym(model)))
    base = zeros(Float64, model.n)
    for i in 1:model.d; base[i] = log10(get(totals, qsyms[i], 1.0)); end
    for j in 1:model.r; base[model.d + j] = log10(kd[j]); end
    base[input_idx] = log10(operating)
    out_at(la) = begin
        p = copy(base); p[adjust_idx] = la
        status = Ref{Symbol}(:not_run)
        value = qK2x(
            model, p;
            input_logspace=true,
            output_logspace=true,
            status=status,
        )[output_idx]
        status[] === :success && isfinite(value) ||
            throw(DomainError(la, "equilibrium solve did not converge to a finite output"))
        value   # log10[output]
    end
    tgt = log10(target)
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
            target_level = target, achieved_level = exp10(out_at(la)),
            kd = collect(Float64.(kd)), totals = nt)
end
# POST /api/placer_level — { rules|session, input_sym, output_sym, kd, totals, operating_input, target_level, adjust_sym }
function handle_placer_level(req)
    body = read_json(req)
    rules = if _raw_haskey(body, :rules)
        _placer_rules(_raw_get(body, :rules, nothing))
    else
        bundle, err = _resolve_bundle_or_response(body); err === nothing || return err
        String.(collect(bundle["rules"]))
    end
    isempty(rules) && return error_response("No reactions"; status = 400)
    enforce_sync_rule_budget(rules)
    for k in (:input_sym, :output_sym, :operating_input, :target_level, :adjust_sym, :kd)
        _raw_haskey(body, k) || return error_response("`$k` is required"; status = 400)
    end
    input_sym  = Symbol(_placer_required_string(body, :input_sym))
    output_sym = Symbol(_placer_required_string(body, :output_sym))
    kd = _placer_numeric_vector(
        _raw_get(body, :kd, nothing), "kd";
        expected_length=length(rules), positive=true)
    totals = _placer_totals_dict(_raw_get(body, :totals, nothing))
    operating_input = _placer_finite_number(
        _raw_get(body, :operating_input, nothing), "operating_input"; positive=true)
    target_level = _placer_finite_number(
        _raw_get(body, :target_level, nothing), "target_level"; positive=true)
    adjust_sym = Symbol(_placer_required_string(body, :adjust_sym))
    res = try
        placer_level(rules, kd, totals, input_sym, output_sym,
                     operating_input, target_level, adjust_sym)
    catch e
        e isa SyncBudgetExceeded && rethrow()
        return error_response("Level solve failed: $(sprint(showerror, e))"; status = 400)
    end
    kd_vec = collect(Float64.(res.kd))
    curve = try
        placer_dose_response(rules, kd_vec, res.totals, input_sym, String(output_sym))
    catch e
        e isa SyncBudgetExceeded && rethrow()
        return error_response("Dose-response curve failed: $(sprint(showerror, e))"; status = 400)
    end
    return json_response(Dict(
        "adjusted_total" => string(res.adjusted_total), "adjusted_value" => res.adjusted_value,
        "target_level" => res.target_level, "achieved_level" => json_safe_real(res.achieved_level),
        "feasible" => res.feasible, "totals" => Dict(string(k) => v for (k, v) in res.totals),
        "dose_response_curve" => curve,
    ))
end
