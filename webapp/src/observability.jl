module Observability

# Lightweight metrics + structured-logging utilities. No external Prometheus
# client dependency — the surface here is small enough that pulling one in
# would dwarf the implementation. If we ever outgrow this (millions of
# series, histogram presentation needs, push gateways), swap to the
# Prometheus.jl package; the public interface here mirrors that library's
# vocabulary (counter_inc!, gauge_set!, hist_observe!) so callers don't
# need to change.

using Dates
using Printf
using JSON3

export counter_inc!, gauge_set!, hist_observe!, render_prometheus
export log_request_json, json_logs_enabled

# ─── Storage ───
# All metrics share one ReentrantLock. The expected hot path is one router
# call doing one counter_inc! + one hist_observe!, well under a microsecond
# of locked work, so contention isn't a concern for this app's load.
const _METRICS_LOCK = ReentrantLock()

# Each metric is a Dict from label-tuple to value/state. Label tuples are
# matched structurally, so callers must pass labels in a stable order. The
# schema (label *names*) lives at the render call site so the storage layer
# stays schema-agnostic.
const _COUNTERS = Dict{String, Dict{Tuple, Float64}}()
const _GAUGES   = Dict{String, Dict{Tuple, Float64}}()

# Histogram state: parallel counts vector (cumulative is computed at render
# time, not maintained here), running sum, and total count. Buckets are
# captured at observation time so different metrics can use different sets.
struct HistState
    counts::Vector{Int}
    sum::Base.RefValue{Float64}
    count::Base.RefValue{Int}
    buckets::Vector{Float64}
end
HistState(buckets::Vector{Float64}) =
    HistState(zeros(Int, length(buckets)), Ref(0.0), Ref(0), buckets)

const _HISTS = Dict{String, Dict{Tuple, HistState}}()

# Prometheus-conventional latency buckets (seconds).
const DEFAULT_BUCKETS = Float64[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
                                1.0, 2.5, 5.0, 10.0]

# ─── Mutators ───

function counter_inc!(name::String, labels::Tuple, by::Real = 1.0)
    lock(_METRICS_LOCK) do
        bucket = get!(_COUNTERS, name) do
            Dict{Tuple, Float64}()
        end
        bucket[labels] = get(bucket, labels, 0.0) + Float64(by)
    end
    return nothing
end

function gauge_set!(name::String, labels::Tuple, value::Real)
    lock(_METRICS_LOCK) do
        bucket = get!(_GAUGES, name) do
            Dict{Tuple, Float64}()
        end
        bucket[labels] = Float64(value)
    end
    return nothing
end

function hist_observe!(name::String, labels::Tuple, value::Real;
                       buckets::Vector{Float64} = DEFAULT_BUCKETS)
    lock(_METRICS_LOCK) do
        per_metric = get!(_HISTS, name) do
            Dict{Tuple, HistState}()
        end
        state = get!(per_metric, labels) do
            HistState(copy(buckets))
        end
        v = Float64(value)
        state.sum[] += v
        state.count[] += 1
        for (i, b) in enumerate(state.buckets)
            if v <= b
                state.counts[i] += 1
            end
        end
    end
    return nothing
end

# Test helper — wipes all stored metrics. Tests should call this in setup
# so cross-test pollution doesn't make assertions order-dependent.
function reset_metrics!()
    lock(_METRICS_LOCK) do
        empty!(_COUNTERS)
        empty!(_GAUGES)
        empty!(_HISTS)
    end
end

# ─── Prometheus text rendering ───

_escape_label(v) = replace(string(v),
    "\\" => "\\\\",
    "\"" => "\\\"",
    "\n" => "\\n",
)

function _format_labels(names::Tuple, values::Tuple)
    isempty(names) && return ""
    parts = String[]
    for (n, v) in zip(names, values)
        push!(parts, string(n, "=\"", _escape_label(v), "\""))
    end
    return "{" * join(parts, ",") * "}"
end

# Render with `le` label appended (or substituted in) for histogram buckets.
function _format_labels_with_le(names::Tuple, values::Tuple, le::String)
    if isempty(names)
        return string("{le=\"", le, "\"}")
    end
    parts = String[]
    for (n, v) in zip(names, values)
        push!(parts, string(n, "=\"", _escape_label(v), "\""))
    end
    push!(parts, string("le=\"", le, "\""))
    return "{" * join(parts, ",") * "}"
end

# Format a Float64 the way Prometheus exposition expects: %g for finite,
# explicit tokens for ±Inf and NaN.
function _format_value(v::Real)
    fv = Float64(v)
    isnan(fv) && return "NaN"
    isinf(fv) && return signbit(fv) ? "-Inf" : "+Inf"
    return @sprintf("%g", fv)
end

"""
    render_prometheus(label_schemas) -> String

Render all stored metrics in Prometheus text exposition format (v0.0.4).
`label_schemas` maps each metric name to a tuple of label names (matching
the order callers used in `counter_inc!`/`gauge_set!`/`hist_observe!`). A
missing entry means the metric is unlabeled.

The output is sorted by metric name, then by label-tuple lexicographically,
so successive scrapes of an unchanged process are byte-identical (useful
for diffing in tests).
"""
function render_prometheus(label_schemas::Dict{String, <:Tuple})
    io = IOBuffer()
    lock(_METRICS_LOCK) do
        for name in sort!(collect(keys(_COUNTERS)))
            labels_names = get(label_schemas, name, ())
            println(io, "# TYPE ", name, " counter")
            bucket = _COUNTERS[name]
            for k in sort!(collect(keys(bucket)); by = x -> string.(x))
                println(io, name, _format_labels(labels_names, k),
                        " ", _format_value(bucket[k]))
            end
        end

        for name in sort!(collect(keys(_GAUGES)))
            labels_names = get(label_schemas, name, ())
            println(io, "# TYPE ", name, " gauge")
            bucket = _GAUGES[name]
            for k in sort!(collect(keys(bucket)); by = x -> string.(x))
                println(io, name, _format_labels(labels_names, k),
                        " ", _format_value(bucket[k]))
            end
        end

        for name in sort!(collect(keys(_HISTS)))
            labels_names = get(label_schemas, name, ())
            println(io, "# TYPE ", name, " histogram")
            bucket_dict = _HISTS[name]
            for k in sort!(collect(keys(bucket_dict)); by = x -> string.(x))
                state = bucket_dict[k]
                # state.counts[i] already holds the cumulative count
                # (observations with v <= buckets[i]) because hist_observe!
                # bumps every bucket whose threshold the value satisfies.
                # So we emit it directly, no further accumulation.
                for (i, b) in enumerate(state.buckets)
                    println(io, name, "_bucket",
                            _format_labels_with_le(labels_names, k, _format_value(b)),
                            " ", state.counts[i])
                end
                # +Inf bucket is by definition the total observation count.
                println(io, name, "_bucket",
                        _format_labels_with_le(labels_names, k, "+Inf"),
                        " ", state.count[])
                println(io, name, "_sum",
                        _format_labels(labels_names, k),
                        " ", _format_value(state.sum[]))
                println(io, name, "_count",
                        _format_labels(labels_names, k),
                        " ", state.count[])
            end
        end
    end
    return String(take!(io))
end

# ─── Structured request logging ───

"""
    log_request_json(io, fields)

Write one JSON line describing an HTTP request. Best-effort: any encoding
or write failure is swallowed because logging must not break the response
path. Callers should set `level`, `event`, and request-shape fields.
"""
function log_request_json(io::IO, fields)
    try
        s = JSON3.write(fields)
        println(io, s)
    catch
        # Intentionally silent; a logger that throws is worse than no log.
    end
    return nothing
end

"""
    json_logs_enabled() -> Bool

Whether to emit per-request JSON log lines to stderr. Off by default
(legacy DebugLog still works either way); enable in containerized
deployments by setting `BIOCIRCUITS_EXPLORER_JSON_LOGS=1`.
"""
function json_logs_enabled()
    v = lowercase(strip(get(ENV, "BIOCIRCUITS_EXPLORER_JSON_LOGS", "")))
    return v in ("1", "true", "yes", "on")
end

"""
    iso_timestamp() -> String

UTC timestamp suitable for log aggregators: "YYYY-MM-DDTHH:MM:SS.sssZ".
"""
function iso_timestamp()
    return string(now(UTC)) * "Z"
end

end # module
