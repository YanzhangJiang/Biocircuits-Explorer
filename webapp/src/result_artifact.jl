# ─── Result Artifact Envelope (bne-result/v1) ──────────────────────────────────
#
# A self-describing wrapper for computed results, so any artifact can answer:
# what kind it is, which inputs (by content hash) produced it, which algorithm +
# version + config produced it, when, and with what warnings. This is what makes
# results cacheable, reproducible, comparable, and exportable across sessions.
#
# Adoption is *additive*: `artifact_metadata` returns the envelope *without* the
# payload, so it can be attached as a sibling `artifact` field on an existing
# flat response (`attach_artifact!`) without changing any field the frontend
# already reads. The async job path attaches it at the single dispatch point so
# every persisted/shared result (resolved via `result_ref`) is self-describing.
#
# Flat-included into BiocircuitsExplorerBackend; relies on `canonical_hash`,
# `_now_iso_timestamp`, `_raw_get` / `_raw_haskey` (canonicalization.jl),
# `network_ir_hash` / `parse_network_ir` (ir.jl), and `biocircuits_explorer_version`
# (version.jl).

const RESULT_ARTIFACT_SCHEMA_VERSION = "bne-result/v1.0.0"

# SHA-256 over the canonical JSON of any value. Reuses the same canonicalizer as the
# IR hashes (canonicalization.jl), so identical configs/specs hash identically (key
# invariant for cache hits and reproducibility checks).
_canonical_hash(value) = canonical_hash(value)

# Best-effort content hashes of the network(s) referenced by a request/spec, for
# linking a result back to the exact IR it came from. Never throws: anything
# unparseable is simply skipped.
function artifact_network_hashes(body)
    hashes = String[]
    try
        if _raw_haskey(body, :networks)
            for n in _raw_get(body, :networks, Any[])
                try
                    push!(hashes, network_ir_hash(parse_network_ir(n)))
                catch
                end
            end
        elseif _raw_haskey(body, :network)
            try
                push!(hashes, network_ir_hash(parse_network_ir(_raw_get(body, :network, nothing))))
            catch
            end
        end
    catch
    end
    return hashes
end

"""
    artifact_metadata(kind; input_hashes, algorithm_name, config, warnings) -> Dict

The self-describing envelope *without* the payload. Attach it as a sibling
`artifact` field on a result so adoption is non-breaking. `config_hash` is `nothing`
when no `config` is given, else a stable hash of the canonicalized config.
"""
function artifact_metadata(kind::AbstractString;
                           input_hashes = Dict{String, Any}(),
                           algorithm_name::AbstractString = kind,
                           config = nothing,
                           warnings = String[])
    return Dict{String, Any}(
        "artifact_schema_version" => RESULT_ARTIFACT_SCHEMA_VERSION,
        "kind" => String(kind),
        "input_hashes" => Dict{String, Any}(input_hashes),
        "algorithm" => Dict{String, Any}(
            "name" => String(algorithm_name),
            "version" => biocircuits_explorer_version(),
            "config_hash" => config === nothing ? nothing : _canonical_hash(config),
        ),
        "warnings" => collect(warnings),
        "created_at" => _now_iso_timestamp(),
    )
end

"""
    attach_artifact!(result, kind; kwargs...) -> result

Attach the envelope as a sibling `artifact` key on a result dict (existing fields
are untouched). Non-dict results are returned unchanged. Returns `result` for
chaining.
"""
function attach_artifact!(result, kind::AbstractString; kwargs...)
    result isa AbstractDict || return result
    result["artifact"] = artifact_metadata(kind; kwargs...)
    return result
end

"""
    wrap_artifact(kind, result; kwargs...) -> Dict

The full envelope with the payload nested under `result`. Use for *new* response
shapes (not for retrofitting existing flat responses — prefer `attach_artifact!`
there to stay non-breaking).
"""
function wrap_artifact(kind::AbstractString, result; kwargs...)
    env = artifact_metadata(kind; kwargs...)
    env["result"] = result
    return env
end
