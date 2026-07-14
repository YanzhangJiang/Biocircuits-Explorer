# Hard allocation bounds for source-to-sink regime paths constructed by an
# ordinary synchronous Web request. BindingAndCatalysis keeps its offline
# default unlimited; ROP-shape and Atlas local jobs opt into the same values
# explicitly, while other Web construction sites select policy from context.
const MAX_WEB_REGIME_PATHS = 2_000
const MAX_WEB_MATERIALIZED_PATH_NODES = 200_000

function _path_budget_exceeded(err::BindingAndCatalysis.PathEnumerationLimitExceeded)
    _sync_budget_exceeded(
        "Regime-path materialization exceeds the Web runtime hard bound " *
        "($(err.resource)=$(err.maximum)). Narrow the model/change selector " *
        "or use a partitioned offline/atlas-job workflow. This resource-bound " *
        "rejection does not establish scientific infeasibility.",
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

function _path_materialization_hard_bound_exceeded(
    err::BindingAndCatalysis.PathEnumerationLimitExceeded;
    label::AbstractString,
)
    throw(ArgumentError(
        "$(label) exceeded the regime-path materialization hard bound " *
        "($(err.resource)=$(err.maximum)). The omitted path population " *
        "remains unknown; this resource-bound rejection does not establish " *
        "scientific infeasibility.",
    ))
end

"""
    _hard_bounded_siso_paths(args...; label, cancel_check, max_paths,
                             max_total_nodes, kwargs...)

Construct a SISO path population under an explicit hard materialization bound.
Unlike `_bounded_siso_paths`, this policy applies outside synchronous HTTP
request context as well, so local jobs cannot accidentally fall back to the
engine's unlimited offline default.
"""
function _hard_bounded_siso_paths(
    args...;
    label::AbstractString="ROP shape job",
    cancel_check=_no_cancel_check,
    max_paths::Integer=MAX_WEB_REGIME_PATHS,
    max_total_nodes::Integer=MAX_WEB_MATERIALIZED_PATH_NODES,
    kwargs...,
)
    try
        return SISOPaths(
            args...;
            kwargs...,
            cancel_check=cancel_check,
            max_paths=Int(max_paths),
            max_total_nodes=Int(max_total_nodes),
        )
    catch err
        err isa BindingAndCatalysis.PathEnumerationLimitExceeded || rethrow()
        _in_sync_request_context() && return _path_budget_exceeded(err)
        return _path_materialization_hard_bound_exceeded(err; label=label)
    end
end

function _hard_bounded_change_paths(
    args...;
    label::AbstractString="Atlas change-path job",
    cancel_check=_no_cancel_check,
    max_paths::Integer=MAX_WEB_REGIME_PATHS,
    max_total_nodes::Integer=MAX_WEB_MATERIALIZED_PATH_NODES,
    kwargs...,
)
    try
        return ChangePaths(
            args...;
            kwargs...,
            cancel_check=cancel_check,
            max_paths=Int(max_paths),
            max_total_nodes=Int(max_total_nodes),
        )
    catch err
        err isa BindingAndCatalysis.PathEnumerationLimitExceeded || rethrow()
        _in_sync_request_context() && return _path_budget_exceeded(err)
        return _path_materialization_hard_bound_exceeded(err; label=label)
    end
end
