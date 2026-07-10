# Hard allocation bounds for source-to-sink regime paths constructed by an
# ordinary synchronous Web request. BindingAndCatalysis and local jobs keep
# their offline default unlimited; every Web construction site still goes
# through these adapters so the request context selects the policy explicitly.
const MAX_WEB_REGIME_PATHS = 2_000
const MAX_WEB_MATERIALIZED_PATH_NODES = 200_000

function _path_budget_exceeded(err::BindingAndCatalysis.PathEnumerationLimitExceeded)
    _sync_budget_exceeded(
        "Regime-path materialization exceeds the Web runtime limit " *
        "($(err.resource)=$(err.maximum)). Narrow the model/change selector " *
        "or use a partitioned offline/atlas-job workflow.",
    )
end

function _bounded_siso_paths(args...; kwargs...)
    _in_sync_request_context() || return SISOPaths(args...; kwargs...)
    try
        return SISOPaths(
            args...;
            kwargs...,
            max_paths=MAX_WEB_REGIME_PATHS,
            max_total_nodes=MAX_WEB_MATERIALIZED_PATH_NODES,
        )
    catch err
        err isa BindingAndCatalysis.PathEnumerationLimitExceeded || rethrow()
        return _path_budget_exceeded(err)
    end
end

function _bounded_change_paths(args...; kwargs...)
    _in_sync_request_context() || return ChangePaths(args...; kwargs...)
    try
        return ChangePaths(
            args...;
            kwargs...,
            max_paths=MAX_WEB_REGIME_PATHS,
            max_total_nodes=MAX_WEB_MATERIALIZED_PATH_NODES,
        )
    catch err
        err isa BindingAndCatalysis.PathEnumerationLimitExceeded || rethrow()
        return _path_budget_exceeded(err)
    end
end
