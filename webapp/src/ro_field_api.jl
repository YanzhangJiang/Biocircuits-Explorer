# Canonical v1-only HTTP entry point for bounded multi-input RO fields.

function handle_ro_field(req)
    body = read_json(req)
    # Enforce the exactly-one model-source contract before resolution. The
    # router may already have pinned the same bundle for locking, but the
    # handler remains a complete trust boundary when called directly in tests.
    _ro_field_validate_model_source(body)
    bundle, err = _resolve_bundle_or_response(body)
    err === nothing || return err
    return json_response(produce_ro_field(body, bundle))
end
