# Internal helper: like `fixed_qK_or_default`, but uses the raw-JSON accessors
# defined in canonicalization.jl so it can read JSON3 objects without a
# `haskey` method.
function _fixed_qK_or_default_raw(body, model, kd::AbstractVector{<:Real})
    fixed_qK = if _raw_haskey(body, :fixed_qK)
        Float64.(collect(_raw_get(body, :fixed_qK, Float64[])))
    else
        default_log_qK(model, kd)
    end
    length(fixed_qK) == model.n ||
        error("Length of `fixed_qK` must equal the full q/K dimension ($(model.n)).")
    return fixed_qK
end

function handle_debug_logs(req)
    body = read_json(req)
    result = DebugLog.read_logs(
        after_seq = Int(get(body, :after_seq, 0)),
        limit     = Int(get(body, :limit, 300)),
        client_id = debug_client_id_from_request(req),
    )
    return json_response(Dict(
        "entries"  => result.entries,
        "next_seq" => result.next_seq,
        "total"    => result.total,
        "limit"    => result.limit,
    ))
end
