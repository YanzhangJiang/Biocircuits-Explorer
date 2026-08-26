"""Hard limits for exact/PWA regular-extension and boundary analysis."""
struct ROStratifiedFieldLimits
    max_cells::Int
    max_facets::Int
    max_incidence_checks::Int
    max_generators::Int
    max_matrix_elements::Int
end

function ROStratifiedFieldLimits(;
    max_cells::Integer=256,
    max_facets::Integer=512,
    max_incidence_checks::Integer=262_144,
    max_generators::Integer=256,
    max_matrix_elements::Integer=1_048_576,
)
    raw = (
        max_cells,
        max_facets,
        max_incidence_checks,
        max_generators,
        max_matrix_elements,
    )
    all(value -> value > 0, raw) || throw(ArgumentError(
        "all stratified RO-field limits must be positive"))
    all(value -> value <= typemax(Int), raw) || throw(ArgumentError(
        "all stratified RO-field limits must fit in Int"))
    return ROStratifiedFieldLimits(Int.(raw)...)
end

"""Raised before stratified-field analysis exceeds a declared work limit."""
struct ROStratifiedFieldLimitExceeded <: Exception
    phase::Symbol
    requested::BigInt
    limit::Int
end

function Base.showerror(io::IO, err::ROStratifiedFieldLimitExceeded)
    print(io, "stratified RO-field analysis ", err.phase, " requires ",
        err.requested, ", exceeding limit=", err.limit)
end

@inline function _rosf_limit(phase::Symbol, requested::BigInt, limit::Int)
    requested <= limit || throw(ROStratifiedFieldLimitExceeded(
        phase, requested, limit))
    return nothing
end

"""
Evidence that the serialized regular cells define one continuous piecewise-
affine potential over the complete declared box.

The only positive status is `:regular_cell_extension_integrable`; every failed
precondition returns `:unknown`. This object always describes regular limits
only and never claims to include or select a serialized singular branch.
"""
struct RORegularExtensionIntegrabilityCertificate
    status::Symbol
    reasons::Vector{Symbol}
    regular_limit_only::Bool
    includes_singular_branch::Bool
    geometry_tolerance::Union{Nothing,Float64}
    checked_cell_count::Int
    checked_facet_count::Int
    checked_internal_facet_count::Int
    checked_domain_facet_count::Int
    checked_continuity_endpoint_count::Int
    max_affine_continuity_residual::Union{Nothing,Float64}
    singular_stratum_count::Int
end

"""
Classical Jacobian or joint-matrix Clarke-hull generators at one 2D point.

Each matrix is an intact `output_count x 2` matrix from one incident regular
cell. The rows are never independently combined, so MIMO coupling is retained.
`status` is `:classical_jacobian`, `:clarke_joint_matrix_hull`, or `:unknown`.
"""
struct ROJointJacobianQuery2D
    status::Symbol
    reason::Union{Nothing,Symbol}
    point::NTuple{2,Float64}
    generator_cell_ids::Vector{Int}
    jacobian_generators::Vector{Matrix{Float64}}
    facet_ids::Vector{Int}
    singular_stratum_ids::Vector{Int}
    regular_limit_only::Bool
    includes_singular_branch::Bool
end

"""Coupled directional-derivative vectors derived from joint Jacobians."""
struct RODirectionalDerivativeGenerators2D
    status::Symbol
    reason::Union{Nothing,Symbol}
    point::NTuple{2,Float64}
    direction::NTuple{2,Float64}
    generator_cell_ids::Vector{Int}
    derivative_generators::Vector{Vector{Float64}}
    regular_limit_only::Bool
    includes_singular_branch::Bool
end

function _rosf_geometry_tolerances(complex::ROCellComplex2D)
    lower = complex.domain.lower_log10
    upper = complex.domain.upper_log10
    all(isfinite, lower) && all(isfinite, upper) || return nothing
    spans = (upper[1] - lower[1], upper[2] - lower[2])
    all(>(0.0), spans) || return nothing
    minimum_span = min(spans...)
    coordinate_scale = max(
        abs(lower[1]), abs(lower[2]), abs(upper[1]), abs(upper[2]),
        minimum_span,
    )
    length_tolerance = max(
        1e-10 * minimum_span,
        64.0 * eps(Float64) * coordinate_scale,
    )
    length_tolerance <= 1e-5 * minimum_span || return nothing
    domain_area = spans[1] * spans[2]
    area_tolerance = 8.0 * length_tolerance * (spans[1] + spans[2]) +
        128.0 * eps(Float64) * domain_area
    return length_tolerance, area_tolerance, domain_area
end

function _rosf_segment_interval_on_edge(segment, edge, tolerance::Float64)
    a, b = edge
    direction = _ro2_sub(b, a)
    length_squared = _ro2_dot(direction, direction)
    edge_length = sqrt(length_squared)
    edge_length > tolerance || return nothing
    parameter_tolerance = max(
        128.0 * eps(Float64), tolerance / edge_length)
    parameters = Float64[]
    for point in segment
        cross = _ro2_cross(direction, _ro2_sub(point, a))
        abs(cross) / edge_length <= tolerance || return nothing
        parameter = _ro2_dot(_ro2_sub(point, a), direction) / length_squared
        -parameter_tolerance <= parameter <= 1.0 + parameter_tolerance ||
            return nothing
        push!(parameters, clamp(parameter, 0.0, 1.0))
    end
    lower, upper = minmax(parameters...)
    (upper - lower) * edge_length > tolerance || return nothing
    return lower, upper, parameter_tolerance
end

function _rosf_domain_side(endpoints, domain::ROInputDomain2D,
                           tolerance::Float64)
    lower, upper = domain.lower_log10, domain.upper_log10
    all(point -> abs(point[1] - lower[1]) <= tolerance, endpoints) &&
        return :axis1_lower
    all(point -> abs(point[1] - upper[1]) <= tolerance, endpoints) &&
        return :axis1_upper
    all(point -> abs(point[2] - lower[2]) <= tolerance, endpoints) &&
        return :axis2_lower
    all(point -> abs(point[2] - upper[2]) <= tolerance, endpoints) &&
        return :axis2_upper
    return nothing
end

function _rosf_affine_values(label::ROAffineLabel2D, point)
    return label.output_offset + label.reaction_order_matrix * collect(point)
end

function _rosf_unknown_certificate(complex, reasons, tolerance,
    internal_count, domain_count, endpoint_count, max_residual)
    return RORegularExtensionIntegrabilityCertificate(
        :unknown,
        sort!(unique(Symbol.(reasons)); by=string),
        true,
        false,
        tolerance,
        length(complex.cells),
        length(complex.facets),
        internal_count,
        domain_count,
        endpoint_count,
        max_residual,
        length(complex.singular_strata),
    )
end

"""
    certify_ro_regular_extension_integrability(complex;
        limits=ROStratifiedFieldLimits(), cancel_check=()->nothing)

Validate the complete regular-cell PWA extension. In addition to the stored
coverage/ambiguity markers, this checks every cell's unique affine label,
facet incidence and geometric membership, exact cell-edge coverage by facets,
and affine-output continuity at both endpoints of every internal facet.
Endpoint equality is sufficient along a segment because both labels are
affine. Scientific ineligibility returns `:unknown`; resource limits and
cancellation propagate as typed control-flow outcomes.
"""
function certify_ro_regular_extension_integrability(
    complex::ROCellComplex2D;
    limits::ROStratifiedFieldLimits=ROStratifiedFieldLimits(),
    cancel_check=() -> nothing,
)
    cancel_check()
    cell_count = length(complex.cells)
    facet_count = length(complex.facets)
    output_count = length(complex.output_indices)
    _rosf_limit(:cells, BigInt(cell_count), limits.max_cells)
    _rosf_limit(:facets, BigInt(facet_count), limits.max_facets)
    matrix_work =
        BigInt(cell_count) * BigInt(output_count) * 2 +
        BigInt(facet_count) * BigInt(output_count) * 4
    _rosf_limit(:matrix_elements, matrix_work, limits.max_matrix_elements)
    total_edges = sum((BigInt(length(cell.vertices)) for cell in complex.cells);
        init=BigInt(0))
    pair_checks = BigInt(cell_count) * BigInt(max(cell_count - 1, 0)) ÷ 2
    _rosf_limit(:incidence_checks,
        total_edges * BigInt(facet_count) + pair_checks,
        limits.max_incidence_checks)

    reasons = Symbol[]
    complex.coverage_complete || push!(reasons, :coverage_incomplete)
    complex.has_ambiguity && push!(reasons, :ambiguous_complex)
    output_count > 0 || push!(reasons, :missing_outputs)
    allunique(complex.output_indices) || push!(reasons, :duplicate_output_index)

    tolerance_info = _rosf_geometry_tolerances(complex)
    tolerance_info === nothing && return _rosf_unknown_certificate(
        complex,
        vcat(reasons, [:unresolvable_float64_geometry]),
        nothing,
        0,
        0,
        0,
        nothing,
    )
    tolerance, area_tolerance, derived_domain_area = tolerance_info

    isfinite(complex.domain_area) && complex.domain_area > 0 ||
        push!(reasons, :invalid_domain_area)
    isfinite(complex.domain_area) &&
        abs(complex.domain_area - derived_domain_area) <= area_tolerance ||
        push!(reasons, :domain_area_mismatch)
    isfinite(complex.covered_area_sum) ||
        push!(reasons, :nonfinite_covered_area)
    complex.gap_area === nothing && push!(reasons, :unknown_gap_area)
    if complex.gap_area !== nothing
        isfinite(complex.gap_area) || push!(reasons, :nonfinite_gap_area)
        isfinite(complex.gap_area) && abs(complex.gap_area) > area_tolerance &&
            push!(reasons, :positive_area_gap)
    end

    cells_by_id = Dict{Int,ROCell2D}()
    labels_by_id = Dict{Int,ROAffineLabel2D}()
    computed_area_sum = 0.0
    edge_coverage = Dict{Tuple{Int,Int},
        Vector{Tuple{Float64,Float64,Float64,Int}}}()
    domain_lower = complex.domain.lower_log10
    domain_upper = complex.domain.upper_log10
    for cell in complex.cells
        cancel_check()
        if haskey(cells_by_id, cell.id)
            push!(reasons, :duplicate_cell_id)
        else
            cells_by_id[cell.id] = cell
        end
        length(cell.vertices) >= 3 || push!(reasons, :invalid_cell_geometry)
        all(point -> all(isfinite, point), cell.vertices) ||
            push!(reasons, :nonfinite_cell_geometry)
        all(point ->
                domain_lower[1] - tolerance <= point[1] <=
                    domain_upper[1] + tolerance &&
                domain_lower[2] - tolerance <= point[2] <=
                    domain_upper[2] + tolerance,
            cell.vertices) || push!(reasons, :cell_outside_domain)
        length(_ro2_unique_points(cell.vertices, tolerance)) ==
            length(cell.vertices) || push!(reasons, :duplicate_cell_vertex)
        _ro2_signed_area(cell.vertices) > tolerance^2 ||
            push!(reasons, :non_ccw_cell_geometry)
        if length(cell.vertices) >= 3
            for vertex_index in eachindex(cell.vertices)
                previous_index = vertex_index == 1 ?
                    length(cell.vertices) : vertex_index - 1
                following_index = vertex_index == length(cell.vertices) ?
                    1 : vertex_index + 1
                incoming = _ro2_sub(
                    cell.vertices[vertex_index],
                    cell.vertices[previous_index])
                outgoing = _ro2_sub(
                    cell.vertices[following_index],
                    cell.vertices[vertex_index])
                edge_scale = max(
                    1.0,
                    sqrt(_ro2_dot(incoming, incoming)),
                    sqrt(_ro2_dot(outgoing, outgoing)),
                )
                _ro2_cross(incoming, outgoing) >= -tolerance * edge_scale ||
                    push!(reasons, :nonconvex_cell_geometry)
            end
        end
        area = _ro2_area(cell.vertices)
        isfinite(area) && area > tolerance^2 ||
            push!(reasons, :invalid_cell_geometry)
        isfinite(area) && (computed_area_sum += area)
        isfinite(cell.area) && abs(cell.area - area) <= area_tolerance ||
            push!(reasons, :cell_area_mismatch)
        for edge_index in eachindex(cell.vertices)
            edge_coverage[(cell.id, edge_index)] =
                Tuple{Float64,Float64,Float64,Int}[]
        end

        cell.set_valued && push!(reasons, :set_valued_cell)
        length(cell.labels) == 1 || push!(reasons, :invalid_cell_label_count)
        if !cell.set_valued && length(cell.labels) == 1
            label = only(cell.labels)
            if size(label.reaction_order_matrix) != (output_count, 2)
                push!(reasons, :invalid_label_matrix_shape)
            elseif length(label.output_offset) != output_count
                push!(reasons, :invalid_label_offset_shape)
            elseif !all(isfinite, label.reaction_order_matrix) ||
                   !all(isfinite, label.output_offset)
                push!(reasons, :nonfinite_affine_label)
            else
                labels_by_id[cell.id] = label
            end
        end
    end
    isfinite(complex.covered_area_sum) &&
        abs(complex.covered_area_sum - computed_area_sum) <= area_tolerance ||
        push!(reasons, :covered_area_mismatch)
    abs(computed_area_sum - derived_domain_area) <= area_tolerance ||
        push!(reasons, :domain_area_not_covered)

    if cell_count >= 2
        for left_index in 1:(cell_count - 1),
            right_index in (left_index + 1):cell_count
            cancel_check()
            overlap = _ro2_convex_intersection(
                complex.cells[left_index].vertices,
                complex.cells[right_index].vertices,
                tolerance,
            )
            _ro2_area(overlap) <= area_tolerance ||
                push!(reasons, :positive_area_cell_overlap)
        end
    end

    facet_ids = Set{Int}()
    internal_count = 0
    domain_count = 0
    endpoint_count = 0
    max_residual = 0.0
    for facet in complex.facets
        cancel_check()
        facet.id in facet_ids ? push!(reasons, :duplicate_facet_id) :
            push!(facet_ids, facet.id)
        endpoints = facet.endpoints
        all(point -> all(isfinite, point), endpoints) ||
            push!(reasons, :nonfinite_facet_geometry)
        _ro2_distance2(endpoints[1], endpoints[2]) > tolerance^2 ||
            push!(reasons, :zero_length_facet)
        allunique(facet.incident_cell_ids) ||
            push!(reasons, :duplicate_facet_incidence)

        expected_incidence = if facet.kind === :internal
            internal_count += 1
            2
        elseif facet.kind === :domain
            domain_count += 1
            1
        else
            push!(reasons, :unsupported_facet_kind)
            0
        end
        length(facet.incident_cell_ids) == expected_incidence ||
            push!(reasons, facet.kind === :domain ?
                :invalid_domain_facet_incidence :
                :invalid_internal_facet_incidence)
        all(id -> haskey(cells_by_id, id), facet.incident_cell_ids) ||
            push!(reasons, :unknown_incident_cell)

        actual_incident = Int[]
        memberships = Tuple{Int,Int,NTuple{3,Float64}}[]
        for (cell_id, cell) in cells_by_id
            matching = Tuple{Int,NTuple{3,Float64}}[]
            for (edge_index, edge) in enumerate(_ro2_edges(cell.vertices))
                interval = _rosf_segment_interval_on_edge(
                    endpoints, edge, tolerance)
                interval === nothing || push!(matching, (edge_index, interval))
            end
            if length(matching) > 1
                push!(reasons, :facet_matches_multiple_cell_edges)
            elseif length(matching) == 1
                edge_index, interval = only(matching)
                push!(actual_incident, cell_id)
                push!(memberships, (cell_id, edge_index, interval))
            end
        end
        sort!(actual_incident)
        sort(unique(facet.incident_cell_ids)) == actual_incident ||
            push!(reasons, :facet_incidence_geometry_mismatch)

        domain_side = _rosf_domain_side(endpoints, complex.domain, tolerance)
        if facet.kind === :domain
            domain_side === nothing && push!(reasons, :domain_facet_off_boundary)
            facet.domain_side == domain_side ||
                push!(reasons, :domain_side_mismatch)
        elseif facet.kind === :internal
            domain_side === nothing || push!(reasons, :internal_facet_on_domain)
            facet.domain_side === nothing || push!(reasons, :internal_domain_side)
        end

        for (cell_id, edge_index, interval) in memberships
            lower, upper, parameter_tolerance = interval
            push!(edge_coverage[(cell_id, edge_index)],
                (lower, upper, parameter_tolerance, facet.id))
        end

        if facet.kind === :internal && length(facet.incident_cell_ids) == 2 &&
           all(id -> haskey(labels_by_id, id), facet.incident_cell_ids)
            left = labels_by_id[facet.incident_cell_ids[1]]
            right = labels_by_id[facet.incident_cell_ids[2]]
            for point in endpoints
                cancel_check()
                left_values = _rosf_affine_values(left, point)
                right_values = _rosf_affine_values(right, point)
                for output_index in 1:output_count
                    residual = abs(left_values[output_index] -
                        right_values[output_index])
                    max_residual = max(max_residual, residual)
                    scale = max(
                        1.0,
                        abs(left_values[output_index]),
                        abs(right_values[output_index]),
                        abs(left.output_offset[output_index]),
                        abs(right.output_offset[output_index]),
                    )
                    residual <= (1e-10 + 128.0 * eps(Float64)) * scale ||
                        push!(reasons, :discontinuous_affine_output)
                end
                endpoint_count += 1
            end
        end
    end

    for intervals in values(edge_coverage)
        cancel_check()
        if isempty(intervals)
            push!(reasons, :missing_cell_boundary_facet)
            continue
        end
        sort!(intervals; by=item -> (item[1], item[2], item[4]))
        cursor = 0.0
        for (lower, upper, parameter_tolerance, _) in intervals
            lower > cursor + parameter_tolerance &&
                push!(reasons, :uncovered_cell_edge_interval)
            lower < cursor - parameter_tolerance &&
                push!(reasons, :overlapping_cell_edge_facets)
            cursor = max(cursor, upper)
        end
        final_tolerance = maximum(item[3] for item in intervals)
        cursor >= 1.0 - final_tolerance ||
            push!(reasons, :uncovered_cell_edge_interval)
    end
    cancel_check()

    unique_reasons = sort!(unique(reasons); by=string)
    status = isempty(unique_reasons) ?
        :regular_cell_extension_integrable : :unknown
    return RORegularExtensionIntegrabilityCertificate(
        status,
        unique_reasons,
        true,
        false,
        tolerance,
        cell_count,
        facet_count,
        internal_count,
        domain_count,
        endpoint_count,
        max_residual,
        length(complex.singular_strata),
    )
end

function _rosf_query_point(point)
    length(point) == 2 || throw(DimensionMismatch(
        "stratified RO-field query point must contain two coordinates"))
    all(value -> value isa Real && !(value isa Bool), point) ||
        throw(ArgumentError(
            "stratified RO-field query point must contain real values"))
    values = (Float64(point[1]), Float64(point[2]))
    all(isfinite, values) || throw(ArgumentError(
        "stratified RO-field query point must be finite"))
    return values
end

function _rosf_unknown_query(point, reason; facet_ids=Int[], stratum_ids=Int[])
    return ROJointJacobianQuery2D(
        :unknown,
        reason,
        point,
        Int[],
        Matrix{Float64}[],
        sort!(unique(Int.(facet_ids))),
        sort!(unique(Int.(stratum_ids))),
        true,
        false,
    )
end

"""
    query_ro_stratified_jacobian(complex, point;
        limits=ROStratifiedFieldLimits(), cancel_check=()->nothing)

Return one classical regular-cell Jacobian for an interior point, or the joint
matrix generators of the Clarke generalized-Jacobian hull at a boundary.
The query is available only after the regular-extension certificate succeeds.
Singular-regime branches are never selected or inserted into the hull.
"""
function query_ro_stratified_jacobian(
    complex::ROCellComplex2D,
    point;
    limits::ROStratifiedFieldLimits=ROStratifiedFieldLimits(),
    cancel_check=() -> nothing,
)
    query_point = _rosf_query_point(point)
    certificate = certify_ro_regular_extension_integrability(
        complex; limits=limits, cancel_check=cancel_check)
    certificate.status === :regular_cell_extension_integrable ||
        return _rosf_unknown_query(
            query_point, :regular_extension_integrability_unknown)
    cancel_check()
    classification = classify_ro_cell_complex_point(
        complex, query_point; tolerance=certificate.geometry_tolerance)
    if classification.status === :outside_domain
        return _rosf_unknown_query(query_point, :outside_domain)
    elseif classification.status in (:gap, :ambiguity)
        return _rosf_unknown_query(
            query_point,
            classification.status,
            facet_ids=classification.facet_ids,
            stratum_ids=classification.singular_stratum_ids,
        )
    end

    cells_by_id = Dict(cell.id => cell for cell in complex.cells)
    pairs = Tuple{Int,Matrix{Float64}}[]
    for cell_id in sort!(unique(classification.cell_ids))
        cancel_check()
        cell = get(cells_by_id, cell_id, nothing)
        cell === nothing && return _rosf_unknown_query(
            query_point, :unknown_incident_cell)
        length(cell.labels) == 1 || return _rosf_unknown_query(
            query_point, :invalid_incident_label)
        push!(pairs, (cell_id, copy(only(cell.labels).reaction_order_matrix)))
    end
    _rosf_limit(:generators, BigInt(length(pairs)), limits.max_generators)
    isempty(pairs) && return _rosf_unknown_query(
        query_point,
        :no_regular_incident_cell,
        facet_ids=classification.facet_ids,
        stratum_ids=classification.singular_stratum_ids,
    )

    status = classification.status === :cell ?
        :classical_jacobian : :clarke_joint_matrix_hull
    reason = nothing
    if status === :classical_jacobian && length(pairs) != 1
        return _rosf_unknown_query(query_point, :nonunique_classical_cell)
    end
    return ROJointJacobianQuery2D(
        status,
        reason,
        query_point,
        first.(pairs),
        last.(pairs),
        copy(classification.facet_ids),
        copy(classification.singular_stratum_ids),
        true,
        false,
    )
end

function _rosf_direction(direction)
    length(direction) == 2 || throw(DimensionMismatch(
        "stratified RO-field direction must contain two components"))
    all(value -> value isa Real && !(value isa Bool), direction) ||
        throw(ArgumentError(
            "stratified RO-field direction must contain real values"))
    values = (Float64(direction[1]), Float64(direction[2]))
    all(isfinite, values) || throw(ArgumentError(
        "stratified RO-field direction must be finite"))
    hypot(values...) > 0 || throw(ArgumentError(
        "stratified RO-field direction must be nonzero"))
    return values
end

"""
    ro_directional_derivative_generators(query, direction;
        limits=ROStratifiedFieldLimits(), cancel_check=()->nothing)

Multiply every intact joint Jacobian generator by the same direction. One
output vector remains linked to one incident cell, preventing cross-output
Cartesian products that are absent from the Clarke generalized Jacobian.
"""
function ro_directional_derivative_generators(
    query::ROJointJacobianQuery2D,
    direction;
    limits::ROStratifiedFieldLimits=ROStratifiedFieldLimits(),
    cancel_check=() -> nothing,
)
    vector = _rosf_direction(direction)
    query.status in (:classical_jacobian, :clarke_joint_matrix_hull, :unknown) ||
        throw(ArgumentError("unsupported joint Jacobian query status"))
    _rosf_limit(:generators, BigInt(length(query.jacobian_generators)),
        limits.max_generators)
    query.status === :unknown && return RODirectionalDerivativeGenerators2D(
        :unknown,
        query.reason,
        query.point,
        vector,
        Int[],
        Vector{Float64}[],
        true,
        false,
    )
    length(query.generator_cell_ids) == length(query.jacobian_generators) ||
        throw(DimensionMismatch(
            "joint Jacobian generator ids and matrices disagree"))

    derivatives = Vector{Float64}[]
    output_count = isempty(query.jacobian_generators) ? 0 :
        size(first(query.jacobian_generators), 1)
    for matrix in query.jacobian_generators
        cancel_check()
        size(matrix) == (output_count, 2) || throw(DimensionMismatch(
            "joint Jacobian generators must share one output count and have " *
            "two input columns"))
        all(isfinite, matrix) || throw(ArgumentError(
            "joint Jacobian generator must be finite"))
        derivative = matrix * collect(vector)
        all(isfinite, derivative) || throw(OverflowError(
            "directional derivative generator is non-finite"))
        push!(derivatives, derivative)
    end
    cancel_check()
    return RODirectionalDerivativeGenerators2D(
        query.status,
        nothing,
        query.point,
        vector,
        copy(query.generator_cell_ids),
        derivatives,
        true,
        false,
    )
end

"""Convenience point query followed by coupled directional pullback."""
function query_ro_stratified_directional_derivatives(
    complex::ROCellComplex2D,
    point,
    direction;
    limits::ROStratifiedFieldLimits=ROStratifiedFieldLimits(),
    cancel_check=() -> nothing,
)
    query = query_ro_stratified_jacobian(
        complex, point; limits=limits, cancel_check=cancel_check)
    return ro_directional_derivative_generators(
        query, direction; limits=limits, cancel_check=cancel_check)
end
