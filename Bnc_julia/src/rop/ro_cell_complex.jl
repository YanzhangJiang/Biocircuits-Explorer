"""
    ROInputDomain2D

Closed rectangular domain for an exact two-total reaction-order construction.
Coordinates and bounds are log10 values. `fixed_logqK` contains the complete
q/K background; the two swept entries are overwritten by domain coordinates.
"""
struct ROInputDomain2D
    axis_indices::NTuple{2,Int}
    lower_log10::NTuple{2,Float64}
    upper_log10::NTuple{2,Float64}
    fixed_logqK::Vector{Float64}

    function ROInputDomain2D(
        axis_indices::NTuple{2,Int},
        lower_log10::NTuple{2,Float64},
        upper_log10::NTuple{2,Float64},
        fixed_logqK::Vector{Float64},
    )
        axis_indices[1] != axis_indices[2] || throw(ArgumentError(
            "ROInputDomain2D axis indices must be distinct"))
        all(isfinite, lower_log10) || throw(ArgumentError(
            "domain lower bounds must be finite"))
        all(isfinite, upper_log10) || throw(ArgumentError(
            "domain upper bounds must be finite"))
        all(isfinite, fixed_logqK) || throw(ArgumentError(
            "fixed_logqK must be finite"))
        all(lower_log10[i] < upper_log10[i] for i in 1:2) ||
            throw(ArgumentError(
                "each domain lower bound must be strictly below its upper bound"))
        return new(axis_indices, lower_log10, upper_log10, fixed_logqK)
    end
end

function ROInputDomain2D(
    axis_indices,
    lower_log10,
    upper_log10,
    fixed_logqK::AbstractVector{<:Real},
)
    length(axis_indices) == 2 || throw(ArgumentError(
        "ROInputDomain2D requires exactly two ordered input axes"))
    length(lower_log10) == 2 || throw(DimensionMismatch(
        "lower_log10 must contain exactly two coordinates"))
    length(upper_log10) == 2 || throw(DimensionMismatch(
        "upper_log10 must contain exactly two coordinates"))

    axes = (Int(axis_indices[1]), Int(axis_indices[2]))
    axes[1] != axes[2] || throw(ArgumentError(
        "ROInputDomain2D axis indices must be distinct"))
    lower = (Float64(lower_log10[1]), Float64(lower_log10[2]))
    upper = (Float64(upper_log10[1]), Float64(upper_log10[2]))
    fixed = Float64.(fixed_logqK)
    all(isfinite, lower) || throw(ArgumentError("domain lower bounds must be finite"))
    all(isfinite, upper) || throw(ArgumentError("domain upper bounds must be finite"))
    all(isfinite, fixed) || throw(ArgumentError("fixed_logqK must be finite"))
    all(lower[i] < upper[i] for i in 1:2) || throw(ArgumentError(
        "each domain lower bound must be strictly below its upper bound"))
    return ROInputDomain2D(axes, lower, upper, fixed)
end

"""One unique affine output label attached to a full-dimensional 2D cell."""
struct ROAffineLabel2D
    source_regime_ids::Vector{Int}
    reaction_order_matrix::Matrix{Float64}
    output_offset::Vector{Float64}
end

"""
One closed, convex, full-dimensional cell. Multiple source regimes with the
same geometry are combined. `set_valued` is true when those sources induce
more than one unique affine label; no arbitrary first label is selected.
"""
struct ROCell2D
    id::Int
    vertices::Vector{NTuple{2,Float64}}
    area::Float64
    source_regime_ids::Vector{Int}
    labels::Vector{ROAffineLabel2D}
    set_valued::Bool
end

"""A positive-length internal or rectangular-domain facet."""
struct ROFacet2D
    id::Int
    kind::Symbol
    endpoints::NTuple{2,NTuple{2,Float64}}
    incident_cell_ids::Vector{Int}
    singular_stratum_ids::Vector{Int}
    normal::NTuple{2,Float64}
    offset::Float64
    mixed_sign::Bool
    domain_side::Union{Nothing,Symbol}
end

"""
Lower-dimensional intersection of a singular regime, or of a regular regime
that becomes lower-dimensional after fixing the background and clipping the
domain. No reaction-order label is invented for these strata.
"""
struct ROSingularStratum2D
    id::Int
    dimension::Int
    vertices::Vector{NTuple{2,Float64}}
    source_regime_ids::Vector{Int}
    nullities::Vector{Int}
    reasons::Vector{Symbol}
end

"""Hard construction limits for the demonstration-scale exact 2D builder."""
struct ROCellComplexBuildLimits
    max_candidate_regimes::Int
    max_cells::Int
    max_singular_strata::Int
    max_pair_checks::Int
    max_facets::Int
end

function ROCellComplexBuildLimits(;
    max_candidate_regimes::Integer=10_000,
    max_cells::Integer=10_000,
    max_singular_strata::Integer=10_000,
    max_pair_checks::Integer=1_000_000,
    max_facets::Integer=100_000,
)
    values = Int[
        max_candidate_regimes,
        max_cells,
        max_singular_strata,
        max_pair_checks,
        max_facets,
    ]
    all(>(0), values) || throw(ArgumentError(
        "all RO cell-complex build limits must be positive"))
    return ROCellComplexBuildLimits(values...)
end

"""Raised before a declared exact-cell construction budget is exceeded."""
struct ROCellComplexLimitExceeded <: Exception
    phase::Symbol
    requested::BigInt
    limit::Int
end

function Base.showerror(io::IO, err::ROCellComplexLimitExceeded)
    print(io, "RO cell-complex ", err.phase, " requires ", err.requested,
        ", exceeding limit=", err.limit)
end

"""
Exact asymptotic two-input cell complex for one binding-equilibrium model and
fixed full q/K background. A successful return represents a complete regime
enumeration under the supplied hard limits; cancellation or a limit throws.
"""
struct ROCellComplex2D
    domain::ROInputDomain2D
    output_indices::Vector{Int}
    cells::Vector{ROCell2D}
    facets::Vector{ROFacet2D}
    singular_strata::Vector{ROSingularStratum2D}
    candidate_regime_count::Int
    regular_candidate_count::Int
    domain_area::Float64
    covered_area_sum::Float64
    gap_area::Union{Nothing,Float64}
    coverage_complete::Bool
    has_ambiguity::Bool
    geometry_tolerance::Float64
end

# The geometry tolerance is an implementation aid, not a caller-controlled
# completeness allowance.  Keep it small in both absolute log10 coordinates
# and relative to the narrowest side of the declared box.
const _RO2_MAX_GEOMETRY_TOLERANCE = 1e-6
const _RO2_MAX_RELATIVE_GEOMETRY_TOLERANCE = 1e-6

"""Result of classifying one point against an `ROCellComplex2D`."""
struct ROPointClassification2D
    status::Symbol
    cell_ids::Vector{Int}
    cell_relations::Vector{Symbol}
    facet_ids::Vector{Int}
    singular_stratum_ids::Vector{Int}
    labels::Vector{ROAffineLabel2D}
end

mutable struct _ROCellCandidate2D
    vertices::Vector{NTuple{2,Float64}}
    source_regime_ids::Vector{Int}
    labels::Vector{ROAffineLabel2D}
end

mutable struct _ROStratumCandidate2D
    dimension::Int
    vertices::Vector{NTuple{2,Float64}}
    source_regime_ids::Vector{Int}
    nullities::Vector{Int}
    reasons::Vector{Symbol}
end

mutable struct _ROFacetAccumulator2D
    kind::Symbol
    endpoints::NTuple{2,NTuple{2,Float64}}
    incident_cell_ids::Set{Int}
    domain_side::Union{Nothing,Symbol}
end

@inline _ro2_cross(a, b) = a[1] * b[2] - a[2] * b[1]
@inline _ro2_sub(a, b) = (a[1] - b[1], a[2] - b[2])
@inline _ro2_add(a, b) = (a[1] + b[1], a[2] + b[2])
@inline _ro2_scale(a, t) = (a[1] * t, a[2] * t)
@inline _ro2_dot(a, b) = a[1] * b[1] + a[2] * b[2]
@inline _ro2_distance2(a, b) = _ro2_dot(_ro2_sub(a, b), _ro2_sub(a, b))

function _ro2_canonical_point(p, tolerance)
    x = abs(p[1]) <= tolerance ? 0.0 : Float64(p[1])
    y = abs(p[2]) <= tolerance ? 0.0 : Float64(p[2])
    return (x, y)
end

function _ro2_unique_points(points, tolerance)
    ordered = sort!([_ro2_canonical_point(p, tolerance) for p in points])
    result = NTuple{2,Float64}[]
    for point in ordered
        any(existing -> _ro2_distance2(existing, point) <= tolerance^2, result) ||
            push!(result, point)
    end
    return result
end

function _ro2_signed_area(vertices)
    length(vertices) < 3 && return 0.0
    total = 0.0
    for i in eachindex(vertices)
        j = i == length(vertices) ? 1 : i + 1
        total += _ro2_cross(vertices[i], vertices[j])
    end
    return total / 2
end

_ro2_area(vertices) = abs(_ro2_signed_area(vertices))

function _ro2_rotate_to_lexicographic_start!(vertices)
    isempty(vertices) && return vertices
    start = argmin(vertices)
    start == 1 && return vertices
    vertices[:] = vcat(vertices[start:end], vertices[1:start-1])
    return vertices
end

function _ro2_canonical_vertices(points, dimension::Int, tolerance)
    unique_points = _ro2_unique_points(points, tolerance)
    if dimension <= 0
        return isempty(unique_points) ? unique_points : unique_points[1:1]
    elseif dimension == 1
        length(unique_points) <= 2 && return unique_points
        best_pair = (1, 2)
        best_distance = -Inf
        for i in 1:(length(unique_points) - 1), j in (i + 1):length(unique_points)
            distance = _ro2_distance2(unique_points[i], unique_points[j])
            if distance > best_distance
                best_distance = distance
                best_pair = (i, j)
            end
        end
        return sort!([unique_points[best_pair[1]], unique_points[best_pair[2]]])
    end

    length(unique_points) >= 3 || return unique_points
    centroid = (
        sum(p[1] for p in unique_points) / length(unique_points),
        sum(p[2] for p in unique_points) / length(unique_points),
    )
    sort!(unique_points; by=p -> (
        atan(p[2] - centroid[2], p[1] - centroid[1]),
        _ro2_distance2(p, centroid),
    ))
    _ro2_signed_area(unique_points) < 0 && reverse!(unique_points)
    return _ro2_rotate_to_lexicographic_start!(unique_points)
end

function _ro2_key(vertices, dimension::Int, tolerance)
    digits = clamp(ceil(Int, -log10(tolerance)) + 2, 0, 14)
    coordinate(value) = begin
        rounded = round(Float64(value); digits=digits)
        iszero(rounded) ? "0" : string(rounded)
    end
    return string(dimension, ":", join(
        (string(coordinate(p[1]), ",", coordinate(p[2])) for p in vertices),
        ";",
    ))
end

function _ro2_label_key(label::ROAffineLabel2D, tolerance)
    digits = clamp(ceil(Int, -log10(tolerance)) + 2, 0, 14)
    values = vcat(vec(permutedims(label.reaction_order_matrix)), label.output_offset)
    return join((string(round(iszero(v) ? 0.0 : v; digits=digits)) for v in values), ",")
end

function _ro2_unique_labels(labels, tolerance)
    by_key = Dict{String,ROAffineLabel2D}()
    for label in labels
        key = _ro2_label_key(label, tolerance)
        if haskey(by_key, key)
            previous = by_key[key]
            by_key[key] = ROAffineLabel2D(
                sort!(unique(vcat(previous.source_regime_ids, label.source_regime_ids))),
                previous.reaction_order_matrix,
                previous.output_offset,
            )
        else
            by_key[key] = label
        end
    end
    return [by_key[key] for key in sort!(collect(keys(by_key)))]
end

function _ro2_polyhedron_vertices(poly, tolerance)
    representation = MixedMatVRep(vrep(poly))
    size(representation.R, 1) == 0 || error(
        "internal error: rectangular clipping produced an unbounded 2D polyhedron")
    matrix = Matrix{Float64}(representation.V)
    size(matrix, 2) == 2 || error(
        "internal error: expected a two-dimensional sliced polyhedron")
    points = NTuple{2,Float64}[(matrix[i, 1], matrix[i, 2]) for i in axes(matrix, 1)]
    return _ro2_canonical_vertices(points, dim(poly), tolerance)
end

function _ro2_regime_polyhedron(regime, domain::ROInputDomain2D)
    raw_C, raw_C0, nullity = get_C_C0_nullity_qK(regime)
    C = Matrix{Float64}(raw_C)
    C0 = Float64.(raw_C0)
    axes = collect(domain.axis_indices)
    base = copy(domain.fixed_logqK)
    base[axes] .= 0.0

    sliced_C = C[:, axes]
    sliced_C0 = C * base + C0
    lower = domain.lower_log10
    upper = domain.upper_log10
    box_C = [
        1.0  0.0
       -1.0  0.0
        0.0  1.0
        0.0 -1.0
    ]
    box_C0 = [-lower[1], upper[1], -lower[2], upper[2]]
    poly = get_polyhedron(vcat(sliced_C, box_C), vcat(sliced_C0, box_C0), nullity)
    detecthlinearity!(poly)
    return poly, nullity, base
end

function _ro2_affine_label(regime, output_indices, axes, base)
    H, H0 = get_H_H0(regime)
    H === nothing && error("regular regime $(regime.idx) has no affine gain matrix")
    H0 === nothing && error("regular regime $(regime.idx) has no affine offset")
    dense_H = Matrix{Float64}(H)
    return ROAffineLabel2D(
        [regime.idx],
        dense_H[output_indices, collect(axes)],
        vec(dense_H[output_indices, :] * base + Float64.(H0[output_indices])),
    )
end

function _ro2_segment_key(endpoints, tolerance)
    points = sort!([endpoints[1], endpoints[2]])
    return _ro2_key(points, 1, tolerance)
end

function _ro2_segment_overlap(a, b, tolerance)
    p, p2 = a
    q, q2 = b
    direction = _ro2_sub(p2, p)
    length2 = _ro2_dot(direction, direction)
    length2 > tolerance^2 || return nothing
    scale = sqrt(length2)
    abs(_ro2_cross(direction, _ro2_sub(q, p))) <= tolerance * max(1.0, scale) ||
        return nothing
    abs(_ro2_cross(direction, _ro2_sub(q2, p))) <= tolerance * max(1.0, scale) ||
        return nothing

    tq = _ro2_dot(_ro2_sub(q, p), direction) / length2
    tq2 = _ro2_dot(_ro2_sub(q2, p), direction) / length2
    low = max(0.0, min(tq, tq2))
    high = min(1.0, max(tq, tq2))
    (high - low) * scale > tolerance || return nothing
    endpoints = sort!([
        _ro2_canonical_point(_ro2_add(p, _ro2_scale(direction, low)), tolerance),
        _ro2_canonical_point(_ro2_add(p, _ro2_scale(direction, high)), tolerance),
    ])
    return (endpoints[1], endpoints[2])
end

function _ro2_edges(vertices)
    edges = NTuple{2,NTuple{2,Float64}}[]
    for i in eachindex(vertices)
        j = i == length(vertices) ? 1 : i + 1
        push!(edges, (vertices[i], vertices[j]))
    end
    return edges
end

function _ro2_intersection_line(s, e, a, b, tolerance)
    segment_direction = _ro2_sub(e, s)
    clip_direction = _ro2_sub(b, a)
    denominator = _ro2_cross(clip_direction, segment_direction)
    abs(denominator) > tolerance || return e
    t = _ro2_cross(clip_direction, _ro2_sub(a, s)) / denominator
    return _ro2_add(s, _ro2_scale(segment_direction, t))
end

function _ro2_convex_intersection(subject, clip, tolerance)
    output = copy(subject)
    for (a, b) in _ro2_edges(clip)
        input = output
        output = NTuple{2,Float64}[]
        isempty(input) && break
        s = input[end]
        s_inside = _ro2_cross(_ro2_sub(b, a), _ro2_sub(s, a)) >= -tolerance
        for e in input
            e_inside = _ro2_cross(_ro2_sub(b, a), _ro2_sub(e, a)) >= -tolerance
            if e_inside
                if !s_inside
                    push!(output, _ro2_intersection_line(s, e, a, b, tolerance))
                end
                push!(output, e)
            elseif s_inside
                push!(output, _ro2_intersection_line(s, e, a, b, tolerance))
            end
            s = e
            s_inside = e_inside
        end
        output = _ro2_canonical_vertices(output, 2, tolerance)
    end
    return output
end

function _ro2_domain_side(edge, domain, tolerance)
    p, q = edge
    lower, upper = domain.lower_log10, domain.upper_log10
    abs(p[1] - lower[1]) <= tolerance && abs(q[1] - lower[1]) <= tolerance &&
        return :axis1_lower
    abs(p[1] - upper[1]) <= tolerance && abs(q[1] - upper[1]) <= tolerance &&
        return :axis1_upper
    abs(p[2] - lower[2]) <= tolerance && abs(q[2] - lower[2]) <= tolerance &&
        return :axis2_lower
    abs(p[2] - upper[2]) <= tolerance && abs(q[2] - upper[2]) <= tolerance &&
        return :axis2_upper
    return nothing
end

function _ro2_facet_geometry(endpoints, tolerance)
    direction = _ro2_sub(endpoints[2], endpoints[1])
    normal = (direction[2], -direction[1])
    magnitude = sqrt(_ro2_dot(normal, normal))
    magnitude > tolerance || error("internal error: zero-length facet")
    normal = (normal[1] / magnitude, normal[2] / magnitude)
    if normal[1] < -tolerance || (abs(normal[1]) <= tolerance && normal[2] < 0)
        normal = (-normal[1], -normal[2])
    end
    offset = -_ro2_dot(normal, endpoints[1])
    mixed_sign = normal[1] * normal[2] < -(tolerance^2)
    return normal, offset, mixed_sign
end

function _ro2_point_on_segment(point, endpoints, tolerance)
    a, b = endpoints
    direction = _ro2_sub(b, a)
    scale = sqrt(_ro2_dot(direction, direction))
    scale > tolerance || return _ro2_distance2(point, a) <= tolerance^2
    abs(_ro2_cross(direction, _ro2_sub(point, a))) <= tolerance * max(1.0, scale) ||
        return false
    projection = _ro2_dot(_ro2_sub(point, a), direction) / (scale^2)
    return -tolerance <= projection <= 1 + tolerance
end

function _ro2_point_cell_relation(vertices, point, tolerance)
    boundary = false
    for (a, b) in _ro2_edges(vertices)
        direction = _ro2_sub(b, a)
        scale = sqrt(_ro2_dot(direction, direction))
        side = _ro2_cross(direction, _ro2_sub(point, a))
        side < -tolerance * max(1.0, scale) && return :outside
        abs(side) <= tolerance * max(1.0, scale) && (boundary = true)
    end
    return boundary ? :boundary : :interior
end

function _ro2_stratum_contains(stratum, point, tolerance)
    isempty(stratum.vertices) && return false
    stratum.dimension == 0 && return _ro2_distance2(stratum.vertices[1], point) <= tolerance^2
    stratum.dimension == 1 && length(stratum.vertices) >= 2 &&
        return _ro2_point_on_segment(point, (stratum.vertices[1], stratum.vertices[end]), tolerance)
    return false
end

function _ro2_add_facet!(accumulators, kind, endpoints, cell_ids, domain_side,
    tolerance, limits)
    canonical = sort!([endpoints[1], endpoints[2]])
    segment = (canonical[1], canonical[2])
    key = string(kind === :internal ? "I:" : "D:",
        domain_side === nothing ? "" : string(domain_side), ":",
        _ro2_segment_key(segment, tolerance))
    if haskey(accumulators, key)
        union!(accumulators[key].incident_cell_ids, cell_ids)
        return
    end
    requested = BigInt(length(accumulators)) + 1
    requested <= limits.max_facets || throw(ROCellComplexLimitExceeded(
        :facets, requested, limits.max_facets))
    accumulators[key] = _ROFacetAccumulator2D(
        kind, segment, Set(Int.(cell_ids)), domain_side)
end

"""
    build_ro_cell_complex(model, domain, output_indices;
        limits=ROCellComplexBuildLimits(), geometry_tolerance=1e-9,
        cancel_check=_NO_CANCEL_CHECK) -> ROCellComplex2D

Construct the exact asymptotic 2D binding-regime partition obtained by direct
substitution of the two swept total coordinates into every regime condition
`C*qK + C0 >= 0`, followed by intersection with the rectangular domain.

Only two distinct conserved-total axes and species outputs are accepted in this
first exact implementation. Float64 CDD polyhedra are used, so `exact` refers to
complete construction over the declared asymptotic regime population, not to
arbitrary-precision coefficients or finite nonlinear equilibrium behavior.
"""
function build_ro_cell_complex(
    model::Bnc,
    domain::ROInputDomain2D,
    output_indices::AbstractVector{<:Integer};
    limits::ROCellComplexBuildLimits=ROCellComplexBuildLimits(),
    geometry_tolerance::Real=1e-9,
    cancel_check=_NO_CANCEL_CHECK,
)
    tolerance = Float64(geometry_tolerance)
    isfinite(tolerance) && tolerance > 0 || throw(ArgumentError(
        "geometry_tolerance must be finite and positive"))
    spans = (
        domain.upper_log10[1] - domain.lower_log10[1],
        domain.upper_log10[2] - domain.lower_log10[2],
    )
    maximum_tolerance = min(
        _RO2_MAX_GEOMETRY_TOLERANCE,
        _RO2_MAX_RELATIVE_GEOMETRY_TOLERANCE * min(spans...),
    )
    tolerance <= maximum_tolerance || throw(ArgumentError(
        "geometry_tolerance must not exceed $(maximum_tolerance) for this " *
        "domain (absolute cap=$(_RO2_MAX_GEOMETRY_TOLERANCE), relative " *
        "cap=$(_RO2_MAX_RELATIVE_GEOMETRY_TOLERANCE) of the shortest side)"))
    isnothing(model.catalysis) || throw(ArgumentError(
        "exact 2D RO cell complexes currently support binding equilibrium only"))
    axes = domain.axis_indices
    all(i -> 1 <= i <= model.d, axes) || throw(ArgumentError(
        "RO cell-complex axes must be two conserved-total q coordinates in 1:$(model.d)"))
    length(domain.fixed_logqK) == model.n || throw(DimensionMismatch(
        "fixed_logqK must contain all $(model.n) q/K coordinates"))
    outputs = Int.(output_indices)
    isempty(outputs) && throw(ArgumentError("at least one species output is required"))
    length(unique(outputs)) == length(outputs) || throw(ArgumentError(
        "output_indices must be unique and ordered"))
    all(i -> 1 <= i <= model.n, outputs) || throw(BoundsError(1:model.n, outputs))

    cancel_check()
    find_all_regimes!(model; cancel_check=cancel_check)
    cancel_check()
    source_regimes = _bind_regimes_data(model)
    candidate_count = count(is_asymptotic, source_regimes)
    BigInt(candidate_count) <= limits.max_candidate_regimes ||
        throw(ROCellComplexLimitExceeded(
            :candidate_regimes, BigInt(candidate_count), limits.max_candidate_regimes))

    cell_candidates = Dict{String,_ROCellCandidate2D}()
    stratum_candidates = Dict{String,_ROStratumCandidate2D}()
    regular_candidate_count = 0
    for regime in source_regimes
        is_asymptotic(regime) || continue
        cancel_check()
        poly, nullity, base = _ro2_regime_polyhedron(regime, domain)
        isempty(poly) && continue
        dimension = dim(poly)
        dimension < 0 && continue
        vertices = _ro2_polyhedron_vertices(poly, tolerance)
        isempty(vertices) && continue
        key = _ro2_key(vertices, dimension, tolerance)

        if nullity == 0 && dimension == 2 && _ro2_area(vertices) > tolerance^2
            regular_candidate_count += 1
            label = _ro2_affine_label(regime, outputs, axes, base)
            if haskey(cell_candidates, key)
                candidate = cell_candidates[key]
                push!(candidate.source_regime_ids, regime.idx)
                push!(candidate.labels, label)
            else
                requested = BigInt(length(cell_candidates)) + 1
                requested <= limits.max_cells || throw(ROCellComplexLimitExceeded(
                    :cells, requested, limits.max_cells))
                cell_candidates[key] = _ROCellCandidate2D(
                    vertices, [regime.idx], [label])
            end
        else
            reason = nullity > 0 ? :singular_regime : :lower_dimensional_slice
            if haskey(stratum_candidates, key)
                candidate = stratum_candidates[key]
                push!(candidate.source_regime_ids, regime.idx)
                push!(candidate.nullities, nullity)
                push!(candidate.reasons, reason)
            else
                requested = BigInt(length(stratum_candidates)) + 1
                requested <= limits.max_singular_strata ||
                    throw(ROCellComplexLimitExceeded(
                        :singular_strata, requested, limits.max_singular_strata))
                stratum_candidates[key] = _ROStratumCandidate2D(
                    dimension, vertices, [regime.idx], [nullity], [reason])
            end
        end
    end
    cancel_check()

    cells = ROCell2D[]
    for key in sort!(collect(keys(cell_candidates)))
        candidate = cell_candidates[key]
        labels = _ro2_unique_labels(candidate.labels, tolerance)
        push!(cells, ROCell2D(
            length(cells) + 1,
            candidate.vertices,
            _ro2_area(candidate.vertices),
            sort!(unique(candidate.source_regime_ids)),
            labels,
            length(labels) > 1,
        ))
    end
    singular_strata = ROSingularStratum2D[]
    for key in sort!(collect(keys(stratum_candidates)))
        candidate = stratum_candidates[key]
        push!(singular_strata, ROSingularStratum2D(
            length(singular_strata) + 1,
            candidate.dimension,
            candidate.vertices,
            sort!(unique(candidate.source_regime_ids)),
            sort!(unique(candidate.nullities)),
            sort!(unique(candidate.reasons); by=string),
        ))
    end

    # The quadratic gate is checked before allocating a pair collection or
    # entering the first pairwise geometry operation.
    pair_checks = BigInt(length(cells)) * BigInt(max(length(cells) - 1, 0)) ÷ 2
    pair_checks <= limits.max_pair_checks || throw(ROCellComplexLimitExceeded(
        :pair_checks, pair_checks, limits.max_pair_checks))
    cancel_check()

    facet_accumulators = Dict{String,_ROFacetAccumulator2D}()
    positive_area_overlap = false
    if length(cells) >= 2
        for i in 1:(length(cells) - 1), j in (i + 1):length(cells)
            cancel_check()
            cell_i, cell_j = cells[i], cells[j]
            intersection = _ro2_convex_intersection(
                cell_i.vertices, cell_j.vertices, tolerance)
            _ro2_area(intersection) > tolerance^2 &&
                (positive_area_overlap = true)

            for edge_i in _ro2_edges(cell_i.vertices), edge_j in _ro2_edges(cell_j.vertices)
                overlap = _ro2_segment_overlap(edge_i, edge_j, tolerance)
                overlap === nothing || _ro2_add_facet!(
                    facet_accumulators,
                    :internal,
                    overlap,
                    (cell_i.id, cell_j.id),
                    nothing,
                    tolerance,
                    limits,
                )
            end
        end
    end
    for cell in cells
        cancel_check()
        for edge in _ro2_edges(cell.vertices)
            side = _ro2_domain_side(edge, domain, tolerance)
            side === nothing && continue
            _ro2_add_facet!(
                facet_accumulators,
                :domain,
                edge,
                (cell.id,),
                side,
                tolerance,
                limits,
            )
        end
    end
    cancel_check()

    facets = ROFacet2D[]
    for key in sort!(collect(keys(facet_accumulators)))
        accumulator = facet_accumulators[key]
        normal, offset, mixed_sign = _ro2_facet_geometry(
            accumulator.endpoints, tolerance)
        singular_ids = Int[]
        for stratum in singular_strata
            stratum.dimension == 1 || continue
            length(stratum.vertices) >= 2 || continue
            overlap = _ro2_segment_overlap(
                accumulator.endpoints,
                (stratum.vertices[1], stratum.vertices[end]),
                tolerance,
            )
            overlap === nothing || push!(singular_ids, stratum.id)
        end
        push!(facets, ROFacet2D(
            length(facets) + 1,
            accumulator.kind,
            accumulator.endpoints,
            sort!(collect(accumulator.incident_cell_ids)),
            sort!(unique(singular_ids)),
            normal,
            offset,
            mixed_sign,
            accumulator.domain_side,
        ))
    end

    domain_area = spans[1] * spans[2]
    covered_area = sum((cell.area for cell in cells); init=0.0)
    # Coverage certification uses its own domain/Float64-derived tolerance.
    # The caller's geometry tolerance may guide clipping and deduplication but
    # cannot relax the final area claim.
    coordinate_scale = max(
        abs(domain.lower_log10[1]), abs(domain.lower_log10[2]),
        abs(domain.upper_log10[1]), abs(domain.upper_log10[2]), min(spans...),
    )
    certification_length_tolerance = max(
        1e-10 * min(spans...),
        64.0 * eps(Float64) * coordinate_scale,
    )
    certification_resolvable =
        certification_length_tolerance <= 1e-5 * min(spans...)
    area_tolerance = 8.0 * certification_length_tolerance *
        (spans[1] + spans[2]) +
        128.0 * eps(Float64) * domain_area
    coverage_complete = certification_resolvable && !positive_area_overlap &&
        abs(covered_area - domain_area) <= area_tolerance
    gap_area = positive_area_overlap ? nothing : max(0.0, domain_area - covered_area)
    has_ambiguity = positive_area_overlap || any(cell.set_valued for cell in cells)
    cancel_check()

    return ROCellComplex2D(
        domain,
        outputs,
        cells,
        facets,
        singular_strata,
        candidate_count,
        regular_candidate_count,
        domain_area,
        covered_area,
        gap_area,
        coverage_complete,
        has_ambiguity,
        tolerance,
    )
end

"""
    classify_ro_cell_complex_point(complex, point; tolerance=complex.geometry_tolerance)

Classify a log10 input point without guessing through gaps or degeneracies.
Statuses are `:cell`, `:boundary`, `:ambiguity`, `:gap`, or
`:outside_domain`. Boundary and ambiguity results retain all incident cells,
facets, singular strata, and unique affine labels.
"""
function classify_ro_cell_complex_point(
    complex::ROCellComplex2D,
    point;
    tolerance::Real=complex.geometry_tolerance,
)
    length(point) == 2 || throw(DimensionMismatch(
        "RO cell-complex point must contain two log10 coordinates"))
    p = (Float64(point[1]), Float64(point[2]))
    all(isfinite, p) || throw(ArgumentError("classification point must be finite"))
    tol = Float64(tolerance)
    isfinite(tol) && tol > 0 || throw(ArgumentError(
        "classification tolerance must be finite and positive"))
    lower, upper = complex.domain.lower_log10, complex.domain.upper_log10
    if p[1] < lower[1] - tol || p[1] > upper[1] + tol ||
       p[2] < lower[2] - tol || p[2] > upper[2] + tol
        return ROPointClassification2D(
            :outside_domain, Int[], Symbol[], Int[], Int[], ROAffineLabel2D[])
    end

    cell_ids = Int[]
    relations = Symbol[]
    collected_labels = ROAffineLabel2D[]
    set_valued = false
    for cell in complex.cells
        relation = _ro2_point_cell_relation(cell.vertices, p, tol)
        relation === :outside && continue
        push!(cell_ids, cell.id)
        push!(relations, relation)
        append!(collected_labels, cell.labels)
        set_valued |= cell.set_valued
    end
    facet_ids = Int[
        facet.id for facet in complex.facets
        if _ro2_point_on_segment(p, facet.endpoints, tol)
    ]
    singular_ids = Int[
        stratum.id for stratum in complex.singular_strata
        if _ro2_stratum_contains(stratum, p, tol)
    ]
    labels = _ro2_unique_labels(collected_labels, tol)

    interior_count = count(==(:interior), relations)
    status = if set_valued || interior_count > 1 ||
        (interior_count == 1 && length(cell_ids) > 1)
        :ambiguity
    elseif !isempty(facet_ids) || !isempty(singular_ids) ||
           any(==(:boundary), relations) || length(cell_ids) > 1
        :boundary
    elseif length(cell_ids) == 1
        :cell
    else
        :gap
    end
    return ROPointClassification2D(
        status,
        cell_ids,
        relations,
        sort!(facet_ids),
        sort!(singular_ids),
        labels,
    )
end
