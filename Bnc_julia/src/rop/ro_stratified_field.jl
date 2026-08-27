"""Hard limits for exact/PWA regular-extension and boundary analysis."""
struct ROStratifiedFieldLimits
    max_cells::Int
    max_facets::Int
    max_incidence_checks::Int
    max_generators::Int
    max_matrix_elements::Int
    max_payload_elements::Int
    max_finalization_work::Int
end

function ROStratifiedFieldLimits(;
    max_cells::Integer=256,
    max_facets::Integer=512,
    max_incidence_checks::Integer=262_144,
    max_generators::Integer=256,
    max_matrix_elements::Integer=1_048_576,
    max_payload_elements::Integer=4_194_304,
    max_finalization_work::Integer=16_777_216,
)
    raw = (
        max_cells,
        max_facets,
        max_incidence_checks,
        max_generators,
        max_matrix_elements,
        max_payload_elements,
        max_finalization_work,
    )
    any(value -> value isa Bool, raw) && throw(ArgumentError(
        "stratified RO-field limits must be integers, not Bool"))
    all(value -> value > 0, raw) || throw(ArgumentError(
        "all stratified RO-field limits must be positive"))
    all(value -> value <= typemax(Int), raw) || throw(ArgumentError(
        "all stratified RO-field limits must fit in Int"))
    return ROStratifiedFieldLimits(Int.(raw)...)
end

function _rosf_copy_limits(limits::ROStratifiedFieldLimits)
    return ROStratifiedFieldLimits(
        limits.max_cells,
        limits.max_facets,
        limits.max_incidence_checks,
        limits.max_generators,
        limits.max_matrix_elements,
        limits.max_payload_elements,
        limits.max_finalization_work,
    )
end

function _rosf_cell_limits(limits::ROStratifiedFieldLimits)
    return ROCellComplexBuildLimits(
        max_candidate_regimes=1,
        max_cells=limits.max_cells,
        max_singular_strata=limits.max_facets,
        max_pair_checks=limits.max_incidence_checks,
        max_facets=limits.max_facets,
        max_outputs=limits.max_matrix_elements,
        max_matrix_elements=limits.max_matrix_elements,
        max_payload_elements=limits.max_payload_elements,
        max_finalization_work=limits.max_finalization_work,
    )
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

const _ROSF_CERTIFICATE_REASONS = Set((
    :ambiguous_complex,
    :cell_area_mismatch,
    :cell_outside_domain,
    :coverage_incomplete,
    :covered_area_mismatch,
    :discontinuous_affine_output,
    :domain_area_mismatch,
    :domain_area_not_covered,
    :domain_facet_off_boundary,
    :domain_side_mismatch,
    :duplicate_cell_id,
    :duplicate_cell_vertex,
    :duplicate_facet_id,
    :duplicate_facet_incidence,
    :duplicate_output_index,
    :external_unverified,
    :facet_incidence_geometry_mismatch,
    :facet_matches_multiple_cell_edges,
    :internal_domain_side,
    :internal_facet_on_domain,
    :invalid_cell_geometry,
    :invalid_cell_label_count,
    :invalid_domain_area,
    :invalid_domain_facet_incidence,
    :invalid_internal_facet_incidence,
    :invalid_label_matrix_shape,
    :invalid_label_offset_shape,
    :missing_cell_boundary_facet,
    :missing_outputs,
    :non_ccw_cell_geometry,
    :nonconvex_cell_geometry,
    :nonfinite_affine_label,
    :nonfinite_cell_geometry,
    :nonfinite_covered_area,
    :nonfinite_facet_geometry,
    :nonfinite_gap_area,
    :overlapping_cell_edge_facets,
    :positive_area_cell_overlap,
    :positive_area_gap,
    :set_valued_cell,
    :source_complex_unverified,
    :uncovered_cell_edge_interval,
    :unknown_gap_area,
    :unknown_incident_cell,
    :unresolvable_float64_geometry,
    :unsupported_facet_kind,
    :zero_length_facet,
))

const _ROSF_QUERY_REASONS = Set((
    :ambiguity,
    :external_unverified,
    :gap,
    :invalid_incident_label,
    :no_regular_incident_cell,
    :nonunique_classical_cell,
    :outside_domain,
    :regular_extension_integrability_unknown,
    :unknown_incident_cell,
))

"""
Evidence that the serialized regular cells define one continuous piecewise-
affine potential over the complete declared box.

The only positive status is `:regular_cell_extension_integrable`; every failed
precondition returns `:unknown`. This object always describes regular limits
only and never claims to include or select a serialized singular branch.
"""
struct _ROSFSealToken end
const _ROSF_SEAL_TOKEN = _ROSFSealToken()

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
    source_complex_sha256::Union{Nothing,String}
    admission_limits::ROStratifiedFieldLimits
    content_sha256::String

    function RORegularExtensionIntegrabilityCertificate(
        status::Symbol,
        reasons::Vector{Symbol},
        regular_limit_only::Bool,
        includes_singular_branch::Bool,
        geometry_tolerance::Union{Nothing,Float64},
        checked_cell_count::Int,
        checked_facet_count::Int,
        checked_internal_facet_count::Int,
        checked_domain_facet_count::Int,
        checked_continuity_endpoint_count::Int,
        max_affine_continuity_residual::Union{Nothing,Float64},
        singular_stratum_count::Int,
        source_complex_sha256::Union{Nothing,String},
        limits::ROStratifiedFieldLimits,
        cancel_check,
        ::_ROSFSealToken,
    )
        cancel_check()
        admitted_limits = _rosf_copy_limits(limits)
        _rosf_preflight_certificate(
            reasons,
            checked_cell_count,
            checked_facet_count,
            singular_stratum_count,
            admitted_limits,
        )
        stored_reasons = Symbol[]
        sizehint!(stored_reasons, length(reasons))
        for reason in reasons
            cancel_check()
            push!(stored_reasons, reason)
        end
        _rosf_validate_certificate_state(
            status,
            stored_reasons,
            regular_limit_only,
            includes_singular_branch,
            geometry_tolerance,
            checked_cell_count,
            checked_facet_count,
            checked_internal_facet_count,
            checked_domain_facet_count,
            checked_continuity_endpoint_count,
            max_affine_continuity_residual,
            singular_stratum_count,
            source_complex_sha256,
        )
        content_sha256 = _rosf_certificate_content_sha256(
            status,
            stored_reasons,
            regular_limit_only,
            includes_singular_branch,
            geometry_tolerance,
            checked_cell_count,
            checked_facet_count,
            checked_internal_facet_count,
            checked_domain_facet_count,
            checked_continuity_endpoint_count,
            max_affine_continuity_residual,
            singular_stratum_count,
            source_complex_sha256,
            cancel_check=cancel_check,
        )
        cancel_check()
        return new(
            status,
            stored_reasons,
            regular_limit_only,
            includes_singular_branch,
            geometry_tolerance,
            checked_cell_count,
            checked_facet_count,
            checked_internal_facet_count,
            checked_domain_facet_count,
            checked_continuity_endpoint_count,
            max_affine_continuity_residual,
            singular_stratum_count,
            source_complex_sha256,
            admitted_limits,
            content_sha256,
        )
    end
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
    source_complex_sha256::Union{Nothing,String}
    admission_limits::ROStratifiedFieldLimits
    content_sha256::String

    function ROJointJacobianQuery2D(
        status::Symbol,
        reason::Union{Nothing,Symbol},
        point::NTuple{2,Float64},
        generator_cell_ids::Vector{Int},
        jacobian_generators::Vector{Matrix{Float64}},
        facet_ids::Vector{Int},
        singular_stratum_ids::Vector{Int},
        regular_limit_only::Bool,
        includes_singular_branch::Bool,
        source_complex_sha256::Union{Nothing,String},
        limits::ROStratifiedFieldLimits,
        cancel_check,
        ::_ROSFSealToken,
    )
        cancel_check()
        admitted_limits = _rosf_copy_limits(limits)
        _rosf_preflight_joint_query(
            generator_cell_ids,
            jacobian_generators,
            facet_ids,
            singular_stratum_ids,
            admitted_limits,
            cancel_check,
        )
        stored_generator_ids = _ro2_cancellable_copy(
            generator_cell_ids, cancel_check)
        stored_generators = Matrix{Float64}[]
        sizehint!(stored_generators, length(jacobian_generators))
        for matrix in jacobian_generators
            cancel_check()
            push!(stored_generators,
                _ro2_cancellable_copy(matrix, cancel_check))
        end
        stored_facet_ids = _ro2_cancellable_copy(facet_ids, cancel_check)
        stored_stratum_ids = _ro2_cancellable_copy(
            singular_stratum_ids, cancel_check)
        _rosf_validate_joint_query_state(
            status,
            reason,
            point,
            stored_generator_ids,
            stored_generators,
            stored_facet_ids,
            stored_stratum_ids,
            regular_limit_only,
            includes_singular_branch,
            source_complex_sha256,
            cancel_check=cancel_check,
        )
        cancel_check()
        content_sha256 = _rosf_joint_query_content_sha256(
            status,
            reason,
            point,
            stored_generator_ids,
            stored_generators,
            stored_facet_ids,
            stored_stratum_ids,
            regular_limit_only,
            includes_singular_branch,
            source_complex_sha256,
            cancel_check=cancel_check,
        )
        cancel_check()
        return new(
            status,
            reason,
            point,
            stored_generator_ids,
            stored_generators,
            stored_facet_ids,
            stored_stratum_ids,
            regular_limit_only,
            includes_singular_branch,
            source_complex_sha256,
            admitted_limits,
            content_sha256,
        )
    end
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
    source_complex_sha256::Union{Nothing,String}
    source_query_sha256::Union{Nothing,String}
    admission_limits::ROStratifiedFieldLimits
    content_sha256::String

    function RODirectionalDerivativeGenerators2D(
        status::Symbol,
        reason::Union{Nothing,Symbol},
        point::NTuple{2,Float64},
        direction::NTuple{2,Float64},
        generator_cell_ids::Vector{Int},
        derivative_generators::Vector{Vector{Float64}},
        regular_limit_only::Bool,
        includes_singular_branch::Bool,
        source_complex_sha256::Union{Nothing,String},
        source_query_sha256::Union{Nothing,String},
        limits::ROStratifiedFieldLimits,
        cancel_check,
        ::_ROSFSealToken,
    )
        cancel_check()
        admitted_limits = _rosf_copy_limits(limits)
        _rosf_preflight_directional(
            generator_cell_ids,
            derivative_generators,
            admitted_limits,
            cancel_check,
        )
        stored_generator_ids = _ro2_cancellable_copy(
            generator_cell_ids, cancel_check)
        stored_generators = Vector{Float64}[]
        sizehint!(stored_generators, length(derivative_generators))
        for vector in derivative_generators
            cancel_check()
            push!(stored_generators,
                _ro2_cancellable_copy(vector, cancel_check))
        end
        _rosf_validate_directional_state(
            status,
            reason,
            point,
            direction,
            stored_generator_ids,
            stored_generators,
            regular_limit_only,
            includes_singular_branch,
            source_complex_sha256,
            source_query_sha256,
            cancel_check=cancel_check,
        )
        cancel_check()
        content_sha256 = _rosf_directional_content_sha256(
            status,
            reason,
            point,
            direction,
            stored_generator_ids,
            stored_generators,
            regular_limit_only,
            includes_singular_branch,
            source_complex_sha256,
            source_query_sha256,
            cancel_check=cancel_check,
        )
        cancel_check()
        return new(
            status,
            reason,
            point,
            direction,
            stored_generator_ids,
            stored_generators,
            regular_limit_only,
            includes_singular_branch,
            source_complex_sha256,
            source_query_sha256,
            admitted_limits,
            content_sha256,
        )
    end
end

function _rosf_validate_certificate_state(
    status::Symbol,
    reasons::Vector{Symbol},
    regular_limit_only::Bool,
    includes_singular_branch::Bool,
    geometry_tolerance::Union{Nothing,Float64},
    checked_cell_count::Int,
    checked_facet_count::Int,
    checked_internal_facet_count::Int,
    checked_domain_facet_count::Int,
    checked_continuity_endpoint_count::Int,
    max_affine_continuity_residual::Union{Nothing,Float64},
    singular_stratum_count::Int,
    source_complex_sha256::Union{Nothing,String},
    ;
    limits::ROStratifiedFieldLimits=ROStratifiedFieldLimits(),
    cancel_check=() -> nothing,
)
    source_complex_sha256 === nothing ||
        occursin(r"^[0-9a-f]{64}$", source_complex_sha256) ||
        throw(ArgumentError(
            "certificate source_complex_sha256 must be canonical"))
    status in (:regular_cell_extension_integrable, :unknown) ||
        throw(ArgumentError("unsupported regular-extension certificate status"))
    reasons == sort(reasons; by=string) && allunique(reasons) ||
        throw(ArgumentError("certificate reasons must be sorted and unique"))
    all(reason -> reason in _ROSF_CERTIFICATE_REASONS, reasons) ||
        throw(ArgumentError("certificate contains an unsupported reason"))
    (status === :regular_cell_extension_integrable) == isempty(reasons) ||
        throw(ArgumentError("certificate status and reasons disagree"))
    regular_limit_only && !includes_singular_branch || throw(ArgumentError(
        "regular-extension certificate must remain regular-limit-only"))
    geometry_tolerance === nothing ||
        (isfinite(geometry_tolerance) && geometry_tolerance > 0) ||
        throw(ArgumentError("certificate geometry tolerance must be finite and positive"))
    counts = (
        checked_cell_count,
        checked_facet_count,
        checked_internal_facet_count,
        checked_domain_facet_count,
        checked_continuity_endpoint_count,
        singular_stratum_count,
    )
    all(>=(0), counts) ||
        throw(ArgumentError("certificate counts must be nonnegative"))
    checked_internal_facet_count + checked_domain_facet_count ==
        checked_facet_count || throw(ArgumentError(
        "certificate facet subtype counts must equal the checked facet count"))
    checked_continuity_endpoint_count <= 2 * checked_internal_facet_count ||
        throw(ArgumentError("certificate continuity endpoint count is incoherent"))
    max_affine_continuity_residual === nothing ||
        (isfinite(max_affine_continuity_residual) &&
            max_affine_continuity_residual >= 0) || throw(ArgumentError(
        "certificate continuity residual must be finite and nonnegative"))
    if status === :regular_cell_extension_integrable
        source_complex_sha256 !== nothing || throw(ArgumentError(
            "positive certificate requires source-complex lineage"))
        geometry_tolerance !== nothing || throw(ArgumentError(
            "positive certificate requires a geometry tolerance"))
        max_affine_continuity_residual !== nothing || throw(ArgumentError(
            "positive certificate requires a continuity residual"))
        checked_cell_count > 0 || throw(ArgumentError(
            "positive certificate requires at least one checked cell"))
        checked_domain_facet_count >= 4 || throw(ArgumentError(
            "positive certificate requires rectangular-domain facet evidence"))
        checked_continuity_endpoint_count ==
            2 * checked_internal_facet_count || throw(ArgumentError(
            "positive certificate requires both endpoints of every internal facet"))
    end
    return nothing
end

function _rosf_validate_id_vector(values::Vector{Int}, label::AbstractString)
    all(>(0), values) && issorted(values) && allunique(values) ||
        throw(ArgumentError("$label must be positive, sorted, and unique"))
    return nothing
end

function _rosf_validate_joint_query_state(
    status::Symbol,
    reason::Union{Nothing,Symbol},
    point::NTuple{2,Float64},
    generator_cell_ids::Vector{Int},
    jacobian_generators::Vector{Matrix{Float64}},
    facet_ids::Vector{Int},
    singular_stratum_ids::Vector{Int},
    regular_limit_only::Bool,
    includes_singular_branch::Bool,
    source_complex_sha256::Union{Nothing,String},
    ;
    cancel_check=() -> nothing,
)
    cancel_check()
    source_complex_sha256 === nothing ||
        occursin(r"^[0-9a-f]{64}$", source_complex_sha256) ||
        throw(ArgumentError(
            "joint query source_complex_sha256 must be canonical"))
    status in (:classical_jacobian, :clarke_joint_matrix_hull, :unknown) ||
        throw(ArgumentError("unsupported joint Jacobian query status"))
    all(isfinite, point) ||
        throw(ArgumentError("joint Jacobian query point must be finite"))
    _rosf_validate_id_vector(generator_cell_ids, "generator cell ids")
    _rosf_validate_id_vector(facet_ids, "facet ids")
    _rosf_validate_id_vector(singular_stratum_ids, "singular-stratum ids")
    regular_limit_only && !includes_singular_branch || throw(ArgumentError(
        "joint Jacobian query must remain regular-limit-only"))
    if status === :unknown
        reason !== nothing ||
            throw(ArgumentError("unknown joint Jacobian query requires a reason"))
        reason in _ROSF_QUERY_REASONS || throw(ArgumentError(
            "unknown joint Jacobian query contains an unsupported reason"))
        isempty(generator_cell_ids) && isempty(jacobian_generators) ||
            throw(ArgumentError("unknown joint Jacobian query cannot contain generators"))
        return nothing
    end

    reason === nothing ||
        throw(ArgumentError("known joint Jacobian query cannot contain a reason"))
    source_complex_sha256 !== nothing || throw(ArgumentError(
        "known joint Jacobian query requires source-complex lineage"))
    !isempty(generator_cell_ids) &&
        length(generator_cell_ids) == length(jacobian_generators) ||
        throw(ArgumentError("joint Jacobian generator ids and matrices disagree"))
    if status === :classical_jacobian
        length(jacobian_generators) == 1 || throw(ArgumentError(
            "classical Jacobian query requires one generator"))
        isempty(facet_ids) && isempty(singular_stratum_ids) || throw(
            ArgumentError(
                "classical Jacobian query cannot contain boundary evidence"))
    end
    if status === :clarke_joint_matrix_hull
        !isempty(facet_ids) || !isempty(singular_stratum_ids) || throw(
            ArgumentError(
                "Clarke joint-matrix query requires boundary evidence"))
    end
    output_count = nothing
    for matrix in jacobian_generators
        cancel_check()
        size(matrix, 2) == 2 && size(matrix, 1) > 0 ||
            throw(DimensionMismatch(
                "joint Jacobian generators must be nonempty matrices with two columns"))
        all(isfinite, matrix) ||
            throw(ArgumentError("joint Jacobian generators must be finite"))
        output_count === nothing && (output_count = size(matrix, 1))
        size(matrix, 1) == output_count || throw(DimensionMismatch(
            "joint Jacobian generators must share one output count"))
    end
    return nothing
end

function _rosf_validate_directional_state(
    status::Symbol,
    reason::Union{Nothing,Symbol},
    point::NTuple{2,Float64},
    direction::NTuple{2,Float64},
    generator_cell_ids::Vector{Int},
    derivative_generators::Vector{Vector{Float64}},
    regular_limit_only::Bool,
    includes_singular_branch::Bool,
    source_complex_sha256::Union{Nothing,String},
    source_query_sha256::Union{Nothing,String},
    ;
    cancel_check=() -> nothing,
)
    cancel_check()
    for (value, label) in (
        (source_complex_sha256, "source_complex_sha256"),
        (source_query_sha256, "source_query_sha256"),
    )
        value === nothing || occursin(r"^[0-9a-f]{64}$", value) ||
            throw(ArgumentError(
                "directional result $label must be canonical"))
    end
    status in (:classical_jacobian, :clarke_joint_matrix_hull, :unknown) ||
        throw(ArgumentError("unsupported directional-generator status"))
    all(isfinite, point) ||
        throw(ArgumentError("directional-generator point must be finite"))
    all(isfinite, direction) && hypot(direction...) > 0 ||
        throw(ArgumentError("directional-generator direction must be finite and nonzero"))
    _rosf_validate_id_vector(generator_cell_ids, "directional generator cell ids")
    regular_limit_only && !includes_singular_branch || throw(ArgumentError(
        "directional generators must remain regular-limit-only"))
    if status === :unknown
        reason !== nothing ||
            throw(ArgumentError("unknown directional result requires a reason"))
        reason in _ROSF_QUERY_REASONS || throw(ArgumentError(
            "unknown directional result contains an unsupported reason"))
        isempty(generator_cell_ids) && isempty(derivative_generators) ||
            throw(ArgumentError("unknown directional result cannot contain generators"))
        return nothing
    end

    reason === nothing ||
        throw(ArgumentError("known directional result cannot contain a reason"))
    source_complex_sha256 !== nothing && source_query_sha256 !== nothing ||
        throw(ArgumentError(
            "known directional result requires complex and query lineage"))
    !isempty(generator_cell_ids) &&
        length(generator_cell_ids) == length(derivative_generators) ||
        throw(ArgumentError("directional generator ids and vectors disagree"))
    status === :classical_jacobian && length(derivative_generators) != 1 &&
        throw(ArgumentError("classical directional result requires one generator"))
    output_count = nothing
    for derivative in derivative_generators
        cancel_check()
        !isempty(derivative) && all(isfinite, derivative) ||
            throw(ArgumentError(
                "directional derivative generators must be finite and nonempty"))
        output_count === nothing && (output_count = length(derivative))
        length(derivative) == output_count || throw(DimensionMismatch(
            "directional derivative generators must share one output count"))
    end
    return nothing
end

function _rosf_preflight_certificate(
    reasons::Vector{Symbol},
    checked_cell_count::Int,
    checked_facet_count::Int,
    singular_stratum_count::Int,
    limits::ROStratifiedFieldLimits,
)
    checked_cell_count >= 0 && checked_facet_count >= 0 &&
        singular_stratum_count >= 0 ||
        throw(ArgumentError("certificate counts must be nonnegative"))
    _rosf_limit(:cells, BigInt(checked_cell_count), limits.max_cells)
    _rosf_limit(:facets, BigInt(checked_facet_count), limits.max_facets)
    _rosf_limit(:incidence_checks,
        BigInt(singular_stratum_count) + BigInt(length(reasons)),
        limits.max_incidence_checks)
    payload_elements = BigInt(16 + length(reasons))
    _rosf_limit(:payload_elements, payload_elements,
        limits.max_payload_elements)
    _rosf_limit(:finalization_work, 3 * payload_elements,
        limits.max_finalization_work)
    return nothing
end

function _rosf_preflight_joint_query(
    generator_cell_ids::Vector{Int},
    jacobian_generators::Vector{Matrix{Float64}},
    facet_ids::Vector{Int},
    singular_stratum_ids::Vector{Int},
    limits::ROStratifiedFieldLimits,
    cancel_check=() -> nothing,
)
    cancel_check()
    _rosf_limit(:generators, BigInt(length(jacobian_generators)),
        limits.max_generators)
    _rosf_limit(:facets, BigInt(length(facet_ids)), limits.max_facets)
    _rosf_limit(:incidence_checks,
        BigInt(length(generator_cell_ids)) +
            BigInt(length(singular_stratum_ids)),
        limits.max_incidence_checks)
    matrix_elements = BigInt(0)
    for matrix in jacobian_generators
        cancel_check()
        matrix_elements += BigInt(length(matrix))
        _rosf_limit(:matrix_elements, matrix_elements,
            limits.max_matrix_elements)
    end
    payload_elements = BigInt(16 + length(generator_cell_ids) +
        length(facet_ids) + length(singular_stratum_ids)) + matrix_elements
    _rosf_limit(:payload_elements, payload_elements,
        limits.max_payload_elements)
    _rosf_limit(:finalization_work,
        3 * payload_elements + matrix_elements,
        limits.max_finalization_work)
    cancel_check()
    return nothing
end

function _rosf_preflight_directional(
    generator_cell_ids::Vector{Int},
    derivative_generators::Vector{Vector{Float64}},
    limits::ROStratifiedFieldLimits,
    cancel_check=() -> nothing,
)
    cancel_check()
    _rosf_limit(:generators, BigInt(length(derivative_generators)),
        limits.max_generators)
    _rosf_limit(:incidence_checks, BigInt(length(generator_cell_ids)),
        limits.max_incidence_checks)
    matrix_elements = BigInt(0)
    for derivative in derivative_generators
        cancel_check()
        matrix_elements += BigInt(length(derivative))
        _rosf_limit(:matrix_elements, matrix_elements,
            limits.max_matrix_elements)
    end
    payload_elements = BigInt(16 + length(generator_cell_ids)) +
        matrix_elements
    _rosf_limit(:payload_elements, payload_elements,
        limits.max_payload_elements)
    _rosf_limit(:finalization_work,
        3 * payload_elements + matrix_elements,
        limits.max_finalization_work)
    cancel_check()
    return nothing
end

function _rosf_write_optional_symbol(io::IO, value::Union{Nothing,Symbol})
    _ro2_write_bool(io, value !== nothing)
    value === nothing || _ro2_write_string(io, String(value))
    return nothing
end

function _rosf_write_optional_string(io::IO, value::Union{Nothing,String})
    _ro2_write_bool(io, value !== nothing)
    value === nothing || _ro2_write_string(io, value)
    return nothing
end

function _rosf_write_symbols(io::IO, values::Vector{Symbol},
                             cancel_check=() -> nothing)
    cancel_check()
    write(io, htol(UInt64(length(values))))
    for value in values
        cancel_check()
        _ro2_write_string(io, String(value))
    end
    return nothing
end

function _rosf_certificate_content_sha256(
    status, reasons, regular_limit_only, includes_singular_branch,
    geometry_tolerance, checked_cell_count, checked_facet_count,
    checked_internal_facet_count, checked_domain_facet_count,
    checked_continuity_endpoint_count, max_affine_continuity_residual,
    singular_stratum_count, source_complex_sha256,
    ;
    cancel_check=() -> nothing,
)
    cancel_check()
    io = IOBuffer()
    write(io, codeunits("bne-ro-regular-extension-certificate-2d-memory/v2\0"))
    _rosf_write_optional_string(io, source_complex_sha256)
    _ro2_write_string(io, String(status))
    _rosf_write_symbols(io, reasons, cancel_check)
    _ro2_write_bool(io, regular_limit_only)
    _ro2_write_bool(io, includes_singular_branch)
    _ro2_write_bool(io, geometry_tolerance !== nothing)
    geometry_tolerance === nothing || _ro2_write_float(io, geometry_tolerance)
    for count in (
        checked_cell_count,
        checked_facet_count,
        checked_internal_facet_count,
        checked_domain_facet_count,
        checked_continuity_endpoint_count,
    )
        cancel_check()
        _ro2_write_int(io, count)
    end
    _ro2_write_bool(io, max_affine_continuity_residual !== nothing)
    max_affine_continuity_residual === nothing ||
        _ro2_write_float(io, max_affine_continuity_residual)
    _ro2_write_int(io, singular_stratum_count)
    cancel_check()
    return bytes2hex(SHA.sha256(take!(io)))
end

function _rosf_joint_query_content_sha256(
    status, reason, point, generator_cell_ids, jacobian_generators,
    facet_ids, singular_stratum_ids, regular_limit_only,
    includes_singular_branch, source_complex_sha256,
    ;
    cancel_check=() -> nothing,
)
    cancel_check()
    io = IOBuffer()
    write(io, codeunits("bne-ro-joint-jacobian-query-2d-memory/v2\0"))
    _rosf_write_optional_string(io, source_complex_sha256)
    _ro2_write_string(io, String(status))
    _rosf_write_optional_symbol(io, reason)
    for value in point
        _ro2_write_float(io, value)
    end
    _ro2_write_ints(io, generator_cell_ids, cancel_check)
    write(io, htol(UInt64(length(jacobian_generators))))
    for matrix in jacobian_generators
        cancel_check()
        write(io, htol(UInt64(size(matrix, 1))))
        write(io, htol(UInt64(size(matrix, 2))))
        for value in matrix
            cancel_check()
            _ro2_write_float(io, value)
        end
    end
    _ro2_write_ints(io, facet_ids, cancel_check)
    _ro2_write_ints(io, singular_stratum_ids, cancel_check)
    _ro2_write_bool(io, regular_limit_only)
    _ro2_write_bool(io, includes_singular_branch)
    cancel_check()
    return bytes2hex(SHA.sha256(take!(io)))
end

function _rosf_directional_content_sha256(
    status, reason, point, direction, generator_cell_ids,
    derivative_generators, regular_limit_only, includes_singular_branch,
    source_complex_sha256, source_query_sha256,
    ;
    cancel_check=() -> nothing,
)
    cancel_check()
    io = IOBuffer()
    write(io, codeunits("bne-ro-directional-generators-2d-memory/v2\0"))
    _rosf_write_optional_string(io, source_complex_sha256)
    _rosf_write_optional_string(io, source_query_sha256)
    _ro2_write_string(io, String(status))
    _rosf_write_optional_symbol(io, reason)
    for value in point
        _ro2_write_float(io, value)
    end
    for value in direction
        _ro2_write_float(io, value)
    end
    _ro2_write_ints(io, generator_cell_ids, cancel_check)
    write(io, htol(UInt64(length(derivative_generators))))
    for derivative in derivative_generators
        cancel_check()
        write(io, htol(UInt64(length(derivative))))
        for value in derivative
            cancel_check()
            _ro2_write_float(io, value)
        end
    end
    _ro2_write_bool(io, regular_limit_only)
    _ro2_write_bool(io, includes_singular_branch)
    cancel_check()
    return bytes2hex(SHA.sha256(take!(io)))
end

function RORegularExtensionIntegrabilityCertificate(
    status::Symbol,
    reasons::Vector{Symbol},
    regular_limit_only::Bool,
    includes_singular_branch::Bool,
    geometry_tolerance::Union{Nothing,Float64},
    checked_cell_count::Int,
    checked_facet_count::Int,
    checked_internal_facet_count::Int,
    checked_domain_facet_count::Int,
    checked_continuity_endpoint_count::Int,
    max_affine_continuity_residual::Union{Nothing,Float64},
    singular_stratum_count::Int,
    ;
    limits::ROStratifiedFieldLimits=ROStratifiedFieldLimits(),
    cancel_check=() -> nothing,
)
    status === :unknown || throw(ArgumentError(
        "positive regular-extension certificates are producer-only"))
    reasons == [:external_unverified] && geometry_tolerance === nothing &&
        checked_cell_count == 0 && checked_facet_count == 0 &&
        checked_internal_facet_count == 0 && checked_domain_facet_count == 0 &&
        checked_continuity_endpoint_count == 0 &&
        max_affine_continuity_residual === nothing &&
        singular_stratum_count == 0 || throw(ArgumentError(
        "public unknown certificate must carry only external-unverified state"))
    return RORegularExtensionIntegrabilityCertificate(
        status,
        reasons,
        regular_limit_only,
        includes_singular_branch,
        geometry_tolerance,
        checked_cell_count,
        checked_facet_count,
        checked_internal_facet_count,
        checked_domain_facet_count,
        checked_continuity_endpoint_count,
        max_affine_continuity_residual,
        singular_stratum_count,
        nothing,
        limits,
        cancel_check,
        _ROSF_SEAL_TOKEN,
    )
end

function ROJointJacobianQuery2D(
    status::Symbol,
    reason::Union{Nothing,Symbol},
    point::NTuple{2,Float64},
    generator_cell_ids::Vector{Int},
    jacobian_generators::Vector{Matrix{Float64}},
    facet_ids::Vector{Int},
    singular_stratum_ids::Vector{Int},
    regular_limit_only::Bool,
    includes_singular_branch::Bool,
    ;
    limits::ROStratifiedFieldLimits=ROStratifiedFieldLimits(),
    cancel_check=() -> nothing,
)
    status === :unknown || throw(ArgumentError(
        "known joint Jacobian queries are producer-only"))
    reason === :external_unverified && isempty(generator_cell_ids) &&
        isempty(jacobian_generators) && isempty(facet_ids) &&
        isempty(singular_stratum_ids) || throw(ArgumentError(
        "public unknown joint query must carry only external-unverified state"))
    return ROJointJacobianQuery2D(
        status,
        reason,
        point,
        generator_cell_ids,
        jacobian_generators,
        facet_ids,
        singular_stratum_ids,
        regular_limit_only,
        includes_singular_branch,
        nothing,
        limits,
        cancel_check,
        _ROSF_SEAL_TOKEN,
    )
end

function RODirectionalDerivativeGenerators2D(
    status::Symbol,
    reason::Union{Nothing,Symbol},
    point::NTuple{2,Float64},
    direction::NTuple{2,Float64},
    generator_cell_ids::Vector{Int},
    derivative_generators::Vector{Vector{Float64}},
    regular_limit_only::Bool,
    includes_singular_branch::Bool,
    ;
    limits::ROStratifiedFieldLimits=ROStratifiedFieldLimits(),
    cancel_check=() -> nothing,
)
    status === :unknown || throw(ArgumentError(
        "known directional-generator results are producer-only"))
    reason === :external_unverified && isempty(generator_cell_ids) &&
        isempty(derivative_generators) || throw(ArgumentError(
        "public unknown directional result must carry only external-unverified state"))
    return RODirectionalDerivativeGenerators2D(
        status,
        reason,
        point,
        direction,
        generator_cell_ids,
        derivative_generators,
        regular_limit_only,
        includes_singular_branch,
        nothing,
        nothing,
        limits,
        cancel_check,
        _ROSF_SEAL_TOKEN,
    )
end

function _rosf_assert_certificate_unchanged(
    certificate::RORegularExtensionIntegrabilityCertificate,
    ;
    limits::ROStratifiedFieldLimits=
        getfield(certificate, :admission_limits),
    cancel_check=() -> nothing,
)
    cancel_check()
    _rosf_preflight_certificate(
        getfield(certificate, :reasons),
        getfield(certificate, :checked_cell_count),
        getfield(certificate, :checked_facet_count),
        getfield(certificate, :singular_stratum_count),
        limits,
    )
    actual = _rosf_certificate_content_sha256(
        getfield(certificate, :status),
        getfield(certificate, :reasons),
        getfield(certificate, :regular_limit_only),
        getfield(certificate, :includes_singular_branch),
        getfield(certificate, :geometry_tolerance),
        getfield(certificate, :checked_cell_count),
        getfield(certificate, :checked_facet_count),
        getfield(certificate, :checked_internal_facet_count),
        getfield(certificate, :checked_domain_facet_count),
        getfield(certificate, :checked_continuity_endpoint_count),
        getfield(certificate, :max_affine_continuity_residual),
        getfield(certificate, :singular_stratum_count),
        getfield(certificate, :source_complex_sha256),
        cancel_check=cancel_check,
    )
    actual == getfield(certificate, :content_sha256) || throw(ArgumentError(
        "RORegularExtensionIntegrabilityCertificate backing storage changed"))
    return nothing
end

function _rosf_assert_joint_query_unchanged(
    query::ROJointJacobianQuery2D;
    limits::ROStratifiedFieldLimits=getfield(query, :admission_limits),
    cancel_check=() -> nothing,
)
    _rosf_preflight_joint_query(
        getfield(query, :generator_cell_ids),
        getfield(query, :jacobian_generators),
        getfield(query, :facet_ids),
        getfield(query, :singular_stratum_ids),
        limits,
        cancel_check,
    )
    actual = _rosf_joint_query_content_sha256(
        getfield(query, :status),
        getfield(query, :reason),
        getfield(query, :point),
        getfield(query, :generator_cell_ids),
        getfield(query, :jacobian_generators),
        getfield(query, :facet_ids),
        getfield(query, :singular_stratum_ids),
        getfield(query, :regular_limit_only),
        getfield(query, :includes_singular_branch),
        getfield(query, :source_complex_sha256),
        cancel_check=cancel_check,
    )
    actual == getfield(query, :content_sha256) || throw(ArgumentError(
        "ROJointJacobianQuery2D backing storage changed after construction"))
    return nothing
end

function _rosf_assert_directional_unchanged(
    result::RODirectionalDerivativeGenerators2D,
    ;
    limits::ROStratifiedFieldLimits=getfield(result, :admission_limits),
    cancel_check=() -> nothing,
)
    _rosf_preflight_directional(
        getfield(result, :generator_cell_ids),
        getfield(result, :derivative_generators),
        limits,
        cancel_check,
    )
    actual = _rosf_directional_content_sha256(
        getfield(result, :status),
        getfield(result, :reason),
        getfield(result, :point),
        getfield(result, :direction),
        getfield(result, :generator_cell_ids),
        getfield(result, :derivative_generators),
        getfield(result, :regular_limit_only),
        getfield(result, :includes_singular_branch),
        getfield(result, :source_complex_sha256),
        getfield(result, :source_query_sha256),
        cancel_check=cancel_check,
    )
    actual == getfield(result, :content_sha256) || throw(ArgumentError(
        "RODirectionalDerivativeGenerators2D backing storage changed"))
    return nothing
end

"""Replay a certificate against its exact sealed source complex."""
function validate_ro_regular_extension_integrability_certificate(
    complex::ROCellComplex2D,
    certificate::RORegularExtensionIntegrabilityCertificate;
    limits::ROStratifiedFieldLimits=
        getfield(certificate, :admission_limits),
    cancel_check=() -> nothing,
)
    cancel_check()
    _ro2_assert_unchanged(complex; cancel_check=cancel_check)
    _rosf_assert_certificate_unchanged(
        certificate; limits=limits, cancel_check=cancel_check)
    source_sha256 = getfield(certificate, :source_complex_sha256)
    source_sha256 !== nothing || throw(ArgumentError(
        "certificate has no producer-bound source-complex lineage"))
    source_sha256 == getfield(complex, :content_sha256) ||
        throw(ArgumentError(
            "certificate source complex does not match replay input"))
    if getfield(certificate, :status) ===
            :regular_cell_extension_integrable
        getfield(complex, :authority_status) === :engine_replayed ||
            throw(ArgumentError(
                "positive certificate requires an engine-replayed source complex"))
    end
    replayed = certify_ro_regular_extension_integrability(
        complex; limits=limits, cancel_check=cancel_check)
    getfield(replayed, :content_sha256) ==
        getfield(certificate, :content_sha256) || throw(ArgumentError(
        "certificate does not match source-bound replay"))
    cancel_check()
    return certificate
end

"""Replay a joint-Jacobian query against its exact sealed source complex."""
function validate_ro_joint_jacobian_query(
    complex::ROCellComplex2D,
    query::ROJointJacobianQuery2D;
    limits::ROStratifiedFieldLimits=getfield(query, :admission_limits),
    cancel_check=() -> nothing,
)
    cancel_check()
    _ro2_assert_unchanged(complex; cancel_check=cancel_check)
    _rosf_assert_joint_query_unchanged(
        query; limits=limits, cancel_check=cancel_check)
    source_sha256 = getfield(query, :source_complex_sha256)
    source_sha256 !== nothing || throw(ArgumentError(
        "joint query has no producer-bound source-complex lineage"))
    source_sha256 == getfield(complex, :content_sha256) ||
        throw(ArgumentError(
            "joint query source complex does not match replay input"))
    if getfield(query, :status) !== :unknown
        getfield(complex, :authority_status) === :engine_replayed ||
            throw(ArgumentError(
                "known joint query requires an engine-replayed source complex"))
    end
    replayed = query_ro_stratified_jacobian(
        complex,
        getfield(query, :point);
        limits=limits,
        cancel_check=cancel_check,
    )
    getfield(replayed, :content_sha256) == getfield(query, :content_sha256) ||
        throw(ArgumentError("joint query does not match source-bound replay"))
    cancel_check()
    return query
end

"""Replay directional generators against the exact sealed source query."""
function validate_ro_directional_derivative_generators(
    query::ROJointJacobianQuery2D,
    result::RODirectionalDerivativeGenerators2D;
    limits::ROStratifiedFieldLimits=getfield(result, :admission_limits),
    cancel_check=() -> nothing,
)
    cancel_check()
    _rosf_assert_joint_query_unchanged(
        query; limits=limits, cancel_check=cancel_check)
    _rosf_assert_directional_unchanged(
        result; limits=limits, cancel_check=cancel_check)
    source_query_sha256 = getfield(result, :source_query_sha256)
    source_query_sha256 !== nothing || throw(ArgumentError(
        "directional result has no producer-bound source-query lineage"))
    source_query_sha256 == getfield(query, :content_sha256) ||
        throw(ArgumentError(
            "directional result source query does not match replay input"))
    getfield(result, :source_complex_sha256) ==
        getfield(query, :source_complex_sha256) || throw(ArgumentError(
        "directional result and source query name different complexes"))
    if getfield(result, :status) !== :unknown
        getfield(result, :source_complex_sha256) !== nothing ||
            throw(ArgumentError(
                "known directional result requires source-complex lineage"))
    end
    replayed = ro_directional_derivative_generators(
        query,
        getfield(result, :direction);
        limits=limits,
        cancel_check=cancel_check,
    )
    getfield(replayed, :content_sha256) == getfield(result, :content_sha256) ||
        throw(ArgumentError(
            "directional result does not match source-bound replay"))
    cancel_check()
    return result
end

function Base.getproperty(
    certificate::RORegularExtensionIntegrabilityCertificate,
    name::Symbol,
)
    _rosf_assert_certificate_unchanged(certificate)
    name === :reasons && return copy(getfield(certificate, :reasons))
    return getfield(certificate, name)
end

function Base.getproperty(query::ROJointJacobianQuery2D, name::Symbol)
    _rosf_assert_joint_query_unchanged(query)
    if name === :generator_cell_ids
        return copy(getfield(query, :generator_cell_ids))
    elseif name === :jacobian_generators
        return [copy(matrix) for matrix in getfield(query, :jacobian_generators)]
    elseif name === :facet_ids
        return copy(getfield(query, :facet_ids))
    elseif name === :singular_stratum_ids
        return copy(getfield(query, :singular_stratum_ids))
    end
    return getfield(query, name)
end

function Base.getproperty(
    result::RODirectionalDerivativeGenerators2D,
    name::Symbol,
)
    _rosf_assert_directional_unchanged(result)
    if name === :generator_cell_ids
        return copy(getfield(result, :generator_cell_ids))
    elseif name === :derivative_generators
        return [copy(vector) for vector in getfield(result, :derivative_generators)]
    end
    return getfield(result, name)
end

function _rosf_geometry_tolerances(complex::ROCellComplex2D)
    domain = getfield(complex, :domain)
    spans, domain_area, length_tolerance, area_tolerance =
        _ro2_certification_tolerances(domain)
    minimum_span = min(spans...)
    length_tolerance <= 1e-5 * minimum_span || return nothing
    return length_tolerance, area_tolerance, domain_area
end

function _rosf_preflight_complex_source(
    complex::ROCellComplex2D,
    limits::ROStratifiedFieldLimits,
    cancel_check,
)
    try
        _ro2_preflight_complex_components(
            getfield(complex, :domain),
            getfield(complex, :output_indices),
            getfield(complex, :cells),
            getfield(complex, :facets),
            getfield(complex, :singular_strata),
            _rosf_cell_limits(limits),
            cancel_check,
        )
    catch error
        error isa ROCellComplexLimitExceeded || rethrow()
        throw(ROStratifiedFieldLimitExceeded(
            error.phase, error.requested, error.limit))
    end
    return nothing
end

function _rosf_segment_interval_on_edge(segment, edge, tolerance::Float64)
    a, b = edge
    direction = _ro2_sub(b, a)
    edge_length = _ro2_distance(a, b)
    edge_length > tolerance || return nothing
    unit = (direction[1] / edge_length, direction[2] / edge_length)
    parameter_tolerance = max(
        128.0 * eps(Float64), tolerance / edge_length)
    parameters = Float64[]
    for point in segment
        perpendicular = unit[1] * (point[2] - a[2]) -
            unit[2] * (point[1] - a[1])
        abs(perpendicular) <= tolerance || return nothing
        parameter = _ro2_dot(_ro2_sub(point, a), unit) / edge_length
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
    internal_count, domain_count, endpoint_count, max_residual;
    limits=ROStratifiedFieldLimits(), cancel_check=() -> nothing)
    return RORegularExtensionIntegrabilityCertificate(
        :unknown,
        sort!(unique(Symbol.(reasons)); by=string),
        true,
        false,
        tolerance,
        length(getfield(complex, :cells)),
        length(getfield(complex, :facets)),
        internal_count,
        domain_count,
        endpoint_count,
        max_residual,
        length(getfield(complex, :singular_strata)),
        getfield(complex, :content_sha256),
        limits,
        cancel_check,
        _ROSF_SEAL_TOKEN,
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
    _rosf_preflight_complex_source(complex, limits, cancel_check)
    _ro2_assert_unchanged(complex; cancel_check=cancel_check)
    cells = getfield(complex, :cells)
    facets = getfield(complex, :facets)
    singular_strata = getfield(complex, :singular_strata)
    output_indices = getfield(complex, :output_indices)
    domain = getfield(complex, :domain)
    cell_count = length(cells)
    facet_count = length(facets)
    output_count = length(output_indices)
    if getfield(complex, :authority_status) !== :engine_replayed
        internal_count = count(
            facet -> getfield(facet, :kind) === :internal, facets)
        domain_count = count(
            facet -> getfield(facet, :kind) === :domain, facets)
        return _rosf_unknown_certificate(
            complex,
            [:source_complex_unverified],
            nothing,
            internal_count,
            domain_count,
            0,
            nothing,
            limits=limits,
            cancel_check=cancel_check,
        )
    end
    _rosf_limit(:cells, BigInt(cell_count), limits.max_cells)
    _rosf_limit(:facets, BigInt(facet_count), limits.max_facets)
    matrix_work =
        BigInt(cell_count) * BigInt(output_count) * 2 +
        BigInt(facet_count) * BigInt(output_count) * 4
    _rosf_limit(:matrix_elements, matrix_work, limits.max_matrix_elements)
    total_edges = BigInt(0)
    sum_squared_vertices = BigInt(0)
    sum_cubed_vertices = BigInt(0)
    for cell in cells
        cancel_check()
        vertex_count = BigInt(length(getfield(cell, :vertices)))
        total_edges += vertex_count
        sum_squared_vertices += vertex_count^2
        sum_cubed_vertices += vertex_count^3
    end
    pair_checks = BigInt(cell_count) * BigInt(max(cell_count - 1, 0)) ÷ 2
    overlap_vertex_work = max(
        BigInt(0),
        (BigInt(cell_count) - 4) * sum_cubed_vertices +
            3 * total_edges * sum_squared_vertices,
    )
    facet_edge_work = total_edges * BigInt(facet_count)
    edge_interval_sort_work = facet_edge_work^2
    incidence_work = total_edges + sum_squared_vertices +
        overlap_vertex_work + facet_edge_work + edge_interval_sort_work +
        pair_checks
    _rosf_limit(:incidence_checks,
        incidence_work,
        limits.max_incidence_checks)
    payload_elements = BigInt(32 + cell_count + facet_count +
        length(singular_strata) + output_count) + total_edges + matrix_work
    _rosf_limit(:payload_elements, payload_elements,
        limits.max_payload_elements)
    _rosf_limit(:finalization_work,
        3 * payload_elements + matrix_work + incidence_work,
        limits.max_finalization_work)
    reasons = Symbol[]
    getfield(complex, :coverage_complete) ||
        push!(reasons, :coverage_incomplete)
    getfield(complex, :has_ambiguity) && push!(reasons, :ambiguous_complex)
    output_count > 0 || push!(reasons, :missing_outputs)
    allunique(output_indices) || push!(reasons, :duplicate_output_index)

    tolerance_info = _rosf_geometry_tolerances(complex)
    if tolerance_info === nothing
        internal_count = 0
        domain_count = 0
        for facet in facets
            cancel_check()
            if getfield(facet, :kind) === :internal
                internal_count += 1
            elseif getfield(facet, :kind) === :domain
                domain_count += 1
            end
        end
        return _rosf_unknown_certificate(
            complex,
            vcat(reasons, [:unresolvable_float64_geometry]),
            nothing,
            internal_count,
            domain_count,
            0,
            nothing,
            limits=limits,
            cancel_check=cancel_check,
        )
    end
    tolerance, area_tolerance, derived_domain_area = tolerance_info

    domain_area = getfield(complex, :domain_area)
    covered_area_sum = getfield(complex, :covered_area_sum)
    gap_area = getfield(complex, :gap_area)
    isfinite(domain_area) && domain_area > 0 ||
        push!(reasons, :invalid_domain_area)
    isfinite(domain_area) &&
        abs(domain_area - derived_domain_area) <= area_tolerance ||
        push!(reasons, :domain_area_mismatch)
    isfinite(covered_area_sum) ||
        push!(reasons, :nonfinite_covered_area)
    gap_area === nothing && push!(reasons, :unknown_gap_area)
    if gap_area !== nothing
        isfinite(gap_area) || push!(reasons, :nonfinite_gap_area)
        isfinite(gap_area) && abs(gap_area) > area_tolerance &&
            push!(reasons, :positive_area_gap)
    end

    cells_by_id = Dict{Int,ROCell2D}()
    labels_by_id = Dict{Int,ROAffineLabel2D}()
    computed_area_sum = 0.0
    edge_coverage = Dict{Tuple{Int,Int},
        Vector{Tuple{Float64,Float64,Float64,Int}}}()
    domain_lower = domain.lower_log10
    domain_upper = domain.upper_log10
    for cell in cells
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
        length(_ro2_unique_points(
            cell.vertices, tolerance, cancel_check)) ==
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
                _ro2_cross(incoming, outgoing) >= -area_tolerance ||
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
    isfinite(covered_area_sum) &&
        abs(covered_area_sum - computed_area_sum) <= area_tolerance ||
        push!(reasons, :covered_area_mismatch)
    abs(computed_area_sum - derived_domain_area) <= area_tolerance ||
        push!(reasons, :domain_area_not_covered)

    if cell_count >= 2
        for left_index in 1:(cell_count - 1),
            right_index in (left_index + 1):cell_count
            cancel_check()
            overlap = _ro2_convex_intersection(
                cells[left_index].vertices,
                cells[right_index].vertices,
                tolerance,
                cancel_check,
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
    for facet in facets
        cancel_check()
        facet.id in facet_ids ? push!(reasons, :duplicate_facet_id) :
            push!(facet_ids, facet.id)
        endpoints = facet.endpoints
        all(point -> all(isfinite, point), endpoints) ||
            push!(reasons, :nonfinite_facet_geometry)
        _ro2_distance(endpoints[1], endpoints[2]) > tolerance ||
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
            for (edge_index, edge) in enumerate(_ro2_edges(
                cell.vertices, cancel_check))
                cancel_check()
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

        domain_side = _rosf_domain_side(endpoints, domain, tolerance)
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
        length(singular_strata),
        getfield(complex, :content_sha256),
        limits,
        cancel_check,
        _ROSF_SEAL_TOKEN,
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

function _rosf_unknown_query(point, reason, source_complex_sha256;
    facet_ids=Int[], stratum_ids=Int[],
    limits=ROStratifiedFieldLimits(), cancel_check=() -> nothing)
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
        source_complex_sha256,
        limits,
        cancel_check,
        _ROSF_SEAL_TOKEN,
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
    cancel_check()
    query_point = _rosf_query_point(point)
    source_complex_sha256 = getfield(complex, :content_sha256)
    unknown_query(reason; facet_ids=Int[], stratum_ids=Int[]) =
        _rosf_unknown_query(
            query_point,
            reason,
            source_complex_sha256;
            facet_ids=facet_ids,
            stratum_ids=stratum_ids,
            limits=limits,
            cancel_check=cancel_check,
        )
    certificate = certify_ro_regular_extension_integrability(
        complex; limits=limits, cancel_check=cancel_check)
    getfield(certificate, :status) === :regular_cell_extension_integrable ||
        return unknown_query(:regular_extension_integrability_unknown)
    cancel_check()
    classification = classify_ro_cell_complex_point(
        complex,
        query_point;
        tolerance=getfield(certificate, :geometry_tolerance),
        limits=_rosf_cell_limits(limits),
        cancel_check=cancel_check,
    )
    getfield(classification, :source_complex_sha256) ==
        source_complex_sha256 || throw(ArgumentError(
        "point classification source does not match stratified query source"))
    getfield(classification, :source_authority_status) === :engine_replayed ||
        return unknown_query(:regular_extension_integrability_unknown)
    classification_status = getfield(classification, :status)
    classification_facet_ids = getfield(classification, :facet_ids)
    classification_stratum_ids = getfield(
        classification, :singular_stratum_ids)
    if classification_status === :outside_domain
        return unknown_query(:outside_domain)
    elseif classification_status in (:gap, :ambiguity)
        return unknown_query(
            classification_status,
            facet_ids=classification_facet_ids,
            stratum_ids=classification_stratum_ids,
        )
    end

    cells_by_id = Dict{Int,ROCell2D}()
    for cell in getfield(complex, :cells)
        cancel_check()
        cells_by_id[getfield(cell, :id)] = cell
    end
    pairs = Tuple{Int,Matrix{Float64}}[]
    classification_cell_ids = getfield(classification, :cell_ids)
    _rosf_limit(:generators, BigInt(length(classification_cell_ids)),
        limits.max_generators)
    matrix_elements = BigInt(0)
    for cell_id in classification_cell_ids
        cancel_check()
        cell = get(cells_by_id, cell_id, nothing)
        cell === nothing && return unknown_query(:unknown_incident_cell)
        labels = getfield(cell, :labels)
        length(labels) == 1 || return unknown_query(:invalid_incident_label)
        matrix = getfield(only(labels), :reaction_order_matrix)
        matrix_elements += BigInt(length(matrix))
        _rosf_limit(:matrix_elements, matrix_elements,
            limits.max_matrix_elements)
        _rosf_limit(:payload_elements,
            BigInt(16 + length(classification_cell_ids) +
                length(classification_facet_ids) +
                length(classification_stratum_ids)) + matrix_elements,
            limits.max_payload_elements)
        push!(pairs, (cell_id,
            _ro2_cancellable_copy(matrix, cancel_check)))
    end
    isempty(pairs) && return unknown_query(
        :no_regular_incident_cell,
        facet_ids=classification_facet_ids,
        stratum_ids=classification_stratum_ids,
    )

    status = classification_status === :cell ?
        :classical_jacobian : :clarke_joint_matrix_hull
    reason = nothing
    if status === :classical_jacobian && length(pairs) != 1
        return unknown_query(:nonunique_classical_cell)
    end
    return ROJointJacobianQuery2D(
        status,
        reason,
        query_point,
        first.(pairs),
        last.(pairs),
        _ro2_cancellable_copy(classification_facet_ids, cancel_check),
        _ro2_cancellable_copy(classification_stratum_ids, cancel_check),
        true,
        false,
        source_complex_sha256,
        limits,
        cancel_check,
        _ROSF_SEAL_TOKEN,
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
    limits::ROStratifiedFieldLimits=getfield(query, :admission_limits),
    cancel_check=() -> nothing,
)
    cancel_check()
    _rosf_assert_joint_query_unchanged(
        query; limits=limits, cancel_check=cancel_check)
    vector = _rosf_direction(direction)
    status = getfield(query, :status)
    reason = getfield(query, :reason)
    point = getfield(query, :point)
    generator_cell_ids = getfield(query, :generator_cell_ids)
    jacobian_generators = getfield(query, :jacobian_generators)
    status === :unknown && return RODirectionalDerivativeGenerators2D(
        :unknown,
        reason,
        point,
        vector,
        Int[],
        Vector{Float64}[],
        true,
        false,
        getfield(query, :source_complex_sha256),
        getfield(query, :content_sha256),
        limits,
        cancel_check,
        _ROSF_SEAL_TOKEN,
    )

    derivatives = Vector{Float64}[]
    output_count = size(first(jacobian_generators), 1)
    for matrix in jacobian_generators
        cancel_check()
        size(matrix) == (output_count, 2) || throw(DimensionMismatch(
            "joint Jacobian generators must share one output count and have " *
            "two input columns"))
        all(isfinite, matrix) || throw(ArgumentError(
            "joint Jacobian generator must be finite"))
        derivative = matrix * collect(vector)
        cancel_check()
        all(isfinite, derivative) || throw(OverflowError(
            "directional derivative generator is non-finite"))
        push!(derivatives, derivative)
    end
    cancel_check()
    return RODirectionalDerivativeGenerators2D(
        status,
        nothing,
        point,
        vector,
        _ro2_cancellable_copy(generator_cell_ids, cancel_check),
        derivatives,
        true,
        false,
        getfield(query, :source_complex_sha256),
        getfield(query, :content_sha256),
        limits,
        cancel_check,
        _ROSF_SEAL_TOKEN,
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
