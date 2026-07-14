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

# Runtime validation mirrors the draft-07 shape owned by
# `schemas/result-artifact.schema.json`.  The schema remains the public source
# of truth; this narrow validator lets the job broker reject a corrupt or
# unrelated remote result before publishing a terminal `succeeded` state.
function _validate_result_artifact_metadata(metadata)
    metadata isa AbstractDict ||
        throw(ArgumentError("Result artifact metadata must be an object."))

    required = ("artifact_schema_version", "kind", "input_hashes", "algorithm", "created_at")
    for key in required
        haskey(metadata, key) ||
            throw(ArgumentError("Result artifact metadata is missing `$(key)`."))
    end

    version = metadata["artifact_schema_version"]
    version isa AbstractString ||
        throw(ArgumentError("Result artifact `artifact_schema_version` must be a string."))
    String(version) == RESULT_ARTIFACT_SCHEMA_VERSION ||
        throw(ArgumentError(
            "Unsupported result artifact schema version: $(version). Expected $(RESULT_ARTIFACT_SCHEMA_VERSION).",
        ))
    metadata["kind"] isa AbstractString ||
        throw(ArgumentError("Result artifact `kind` must be a string."))
    input_hashes = metadata["input_hashes"]
    input_hashes isa AbstractDict ||
        throw(ArgumentError("Result artifact `input_hashes` must be an object."))
    for key in ("network_ir_hash", "design_spec_hash")
        if haskey(input_hashes, key)
            input_hashes[key] isa AbstractString ||
                throw(ArgumentError("Result artifact `input_hashes.$(key)` must be a string."))
        end
    end
    if haskey(input_hashes, "network_ir_hashes")
        network_hashes = input_hashes["network_ir_hashes"]
        network_hashes isa AbstractVector ||
            throw(ArgumentError("Result artifact `input_hashes.network_ir_hashes` must be an array."))
        all(item -> item isa AbstractString, network_hashes) ||
            throw(ArgumentError("Every `input_hashes.network_ir_hashes` item must be a string."))
    end
    metadata["created_at"] isa AbstractString ||
        throw(ArgumentError("Result artifact `created_at` must be a string."))

    algorithm = metadata["algorithm"]
    algorithm isa AbstractDict ||
        throw(ArgumentError("Result artifact `algorithm` must be an object."))
    for key in ("name", "version")
        haskey(algorithm, key) ||
            throw(ArgumentError("Result artifact `algorithm` is missing `$(key)`."))
        algorithm[key] isa AbstractString ||
            throw(ArgumentError("Result artifact `algorithm.$(key)` must be a string."))
    end
    allowed_algorithm_keys = Set(("name", "version", "config_hash"))
    for key in keys(algorithm)
        String(key) in allowed_algorithm_keys ||
            throw(ArgumentError("Result artifact `algorithm` contains unsupported field `$(key)`."))
    end
    if haskey(algorithm, "config_hash")
        config_hash = algorithm["config_hash"]
        (config_hash === nothing || config_hash isa AbstractString) ||
            throw(ArgumentError("Result artifact `algorithm.config_hash` must be a string or null."))
    end

    if haskey(metadata, "warnings")
        warnings = metadata["warnings"]
        warnings isa AbstractVector ||
            throw(ArgumentError("Result artifact `warnings` must be an array."))
        all(item -> item isa AbstractString, warnings) ||
            throw(ArgumentError("Every result artifact warning must be a string."))
    end

    return metadata
end
