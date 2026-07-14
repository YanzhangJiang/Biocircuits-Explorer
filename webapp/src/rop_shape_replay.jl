# Sampled finite-curve verification for ROP shape optimization.
#
# This module deliberately keeps sampled phenotype measurements separate from
# the exact-path polyhedral optimizer.  A passing result says only that every
# requested grid point was valid and that the sampled curve contained two
# resolved peaks under this versioned measurement policy.

const ROP_SHAPE_REPLAY_VERSION = "bne-rop-shape-replay/v1.0.0"

function _rop_shape_crossing(xs::AbstractVector{<:Real}, ys::AbstractVector{<:Real},
                             first_idx::Int, last_idx::Int, level::Float64)
    first_idx == last_idx && return Float64(xs[first_idx])
    step = first_idx < last_idx ? 1 : -1
    i = first_idx
    while i != last_idx
        j = i + step
        yi = Float64(ys[i]) - level
        yj = Float64(ys[j]) - level
        if yi == 0.0
            return Float64(xs[i])
        elseif yj == 0.0
            return Float64(xs[j])
        elseif signbit(yi) != signbit(yj)
            width = Float64(ys[j]) - Float64(ys[i])
            abs(width) <= eps(Float64) && return (Float64(xs[i]) + Float64(xs[j])) / 2
            weight = (level - Float64(ys[i])) / width
            return Float64(xs[i]) + weight * (Float64(xs[j]) - Float64(xs[i]))
        end
        i = j
    end
    return NaN
end

function _rop_shape_local_maxima(values::Vector{Float64})
    length(values) < 3 && return Int[]
    return [i for i in 2:(length(values) - 1)
            if values[i] >= values[i - 1] && values[i] > values[i + 1]]
end

"""
    analyze_two_peak_curve(xs, ys, valid; min_prominence_log10=0.0)

Measure two sampled peaks, their separation, half-prominence widths, and the
central interval between their inner half-prominence crossings.  The function
fails closed on partial, non-finite, unsorted, or under-resolved curves.
"""
function analyze_two_peak_curve(xs::AbstractVector{<:Real}, ys::AbstractVector{<:Real},
                                valid::AbstractVector;
                                min_prominence_log10::Real=0.0)
    n = length(xs)
    base = Dict{String, Any}(
        "schema_version" => ROP_SHAPE_REPLAY_VERSION,
        "sample_points" => n,
        "complete" => false,
        "pass" => false,
    )
    n == length(ys) == length(valid) || return merge(base, Dict(
        "status" => "invalid_shape",
        "reason" => "xs, ys, and valid must have equal lengths",
    ))
    n >= 5 || return merge(base, Dict(
        "status" => "under_resolved",
        "reason" => "two-peak verification requires at least five samples",
    ))
    threshold = Float64(min_prominence_log10)
    (isfinite(threshold) && threshold >= 0.0) || return merge(base, Dict(
        "status" => "invalid_threshold",
        "reason" => "min_prominence_log10 must be finite and nonnegative",
    ))
    all(value -> value === true, valid) || return merge(base, Dict(
        "status" => "partial_solver_failure",
        "reason" => "every replay sample must have literal validity true",
    ))
    x = Float64.(xs)
    y = Float64.(ys)
    (all(isfinite, x) && all(isfinite, y)) || return merge(base, Dict(
        "status" => "nonfinite_sample",
        "reason" => "all replay coordinates must be finite",
    ))
    all(x[i + 1] > x[i] for i in 1:(n - 1)) || return merge(base, Dict(
        "status" => "invalid_grid",
        "reason" => "replay inputs must be strictly increasing",
    ))

    peak_candidates = _rop_shape_local_maxima(y)
    length(peak_candidates) >= 2 || return merge(base, Dict(
        "status" => "two_peaks_not_found",
        "reason" => "fewer than two sampled local maxima were resolved",
        "peak_candidate_count" => length(peak_candidates),
        "complete" => true,
    ))

    # Prefer the pair with the largest weaker prominence.  This keeps a small
    # numerical ripple from displacing one of two well-resolved biological peaks.
    best = nothing
    for left_pos in 1:(length(peak_candidates) - 1)
        for right_pos in (left_pos + 1):length(peak_candidates)
            left_idx = peak_candidates[left_pos]
            right_idx = peak_candidates[right_pos]
            valley_idx = left_idx - 1 + argmin(@view y[left_idx:right_idx])
            left_outer_min = minimum(@view y[1:left_idx])
            right_outer_min = minimum(@view y[right_idx:end])
            valley = y[valley_idx]
            left_base = max(left_outer_min, valley)
            right_base = max(right_outer_min, valley)
            left_prominence = y[left_idx] - left_base
            right_prominence = y[right_idx] - right_base
            score = (min(left_prominence, right_prominence),
                     left_prominence + right_prominence,
                     x[right_idx] - x[left_idx])
            if best === nothing || score > best.score
                best = (; left_idx, right_idx, valley_idx, left_base, right_base,
                         left_prominence, right_prominence, score)
            end
        end
    end

    left_level = best.left_base + best.left_prominence / 2
    right_level = best.right_base + best.right_prominence / 2
    left_outer = _rop_shape_crossing(x, y, best.left_idx, 1, left_level)
    left_inner = _rop_shape_crossing(x, y, best.left_idx, best.valley_idx, left_level)
    right_inner = _rop_shape_crossing(x, y, best.right_idx, best.valley_idx, right_level)
    right_outer = _rop_shape_crossing(x, y, best.right_idx, n, right_level)
    crossings = [left_outer, left_inner, right_inner, right_outer]
    all(isfinite, crossings) || return merge(base, Dict(
        "status" => "half_prominence_crossing_missing",
        "reason" => "the sampled window did not bracket every half-prominence crossing",
        "complete" => true,
    ))

    passed = min(best.left_prominence, best.right_prominence) + 1e-12 >= threshold
    return merge(base, Dict{String, Any}(
        "status" => passed ? "pass" : "prominence_below_minimum",
        "reason" => passed ?
            "two complete sampled peaks met the declared prominence" :
            "the weaker sampled peak missed min_prominence_log10",
        "complete" => true,
        "pass" => passed,
        "peak_candidate_count" => length(peak_candidates),
        "peak_indices" => [best.left_idx, best.right_idx],
        "peak_input_log10" => [x[best.left_idx], x[best.right_idx]],
        "peak_output_log10" => [y[best.left_idx], y[best.right_idx]],
        "valley_index" => best.valley_idx,
        "valley_input_log10" => x[best.valley_idx],
        "valley_output_log10" => y[best.valley_idx],
        "peak_separation_log10" => x[best.right_idx] - x[best.left_idx],
        "left_prominence_log10" => best.left_prominence,
        "right_prominence_log10" => best.right_prominence,
        "left_half_prominence_width_log10" => left_inner - left_outer,
        "right_half_prominence_width_log10" => right_outer - right_inner,
        "central_half_prominence_interval_log10" => max(0.0, right_inner - left_inner),
        "half_prominence_crossings_log10" => crossings,
        "min_prominence_log10" => threshold,
    ))
end
