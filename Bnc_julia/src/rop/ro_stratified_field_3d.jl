import JSON3
import SHA

const RO_REGULAR_EXTENSION_INTEGRABILITY_3D_VERSION =
    "bne-ro-regular-extension-integrability-3d/v1.0.0"
const RO_REGULAR_EXTENSION_INTEGRABILITY_3D_SCOPE =
    :explicit_convex_affine_d3_float64_dyadic_exact_global_regular_extension
const RO_REGULAR_EXTENSION_INTEGRABILITY_3D_COORDINATES =
    :float64_bits_interpreted_as_exact_dyadic_rationals
const RO_REGULAR_EXTENSION_INTEGRABILITY_3D_DOMAIN_TOPOLOGY =
    :complete_contractible_box

const _ROSF3_MAX_CELLS = 1_024
const _ROSF3_MAX_FACETS = 1_000_000
const _ROSF3_MAX_OUTPUTS = 1_000_000
const _ROSF3_MAX_EQUATIONS = 1_000_000_000
const _ROSF3_MAX_WITNESSES = 1_000_000
const _ROSF3_MAX_IDENTITY_BYTES = 1_000_000_000
const _ROSF3_MAX_EXACT_BIT_WORK = 2_000_000_000
const _ROSF3_MAX_EXACT_RESIDUAL_BITS = 100_000_000

struct _ROSF3ValidatedToken end
const _ROSF3_VALIDATED = _ROSF3ValidatedToken()

"""Hard bounds for exact D=3 regular-extension analysis."""
struct ROStratifiedField3DLimits
    max_cells::Int
    max_facets::Int
    max_internal_facets::Int
    max_outputs::Int
    max_exact_basis_points::Int
    max_exact_equations::Int
    max_geometry_exact_bit_work::Int
    max_geometry_total_work::Int
    max_exact_bit_work::Int
    max_exact_residual_bits::Int
    max_witnesses::Int
    max_identity_bytes::Int

    function ROStratifiedField3DLimits(::_ROSF3ValidatedToken, args...)
        new(args...)
    end
end

function _rosf3_positive_limit(raw, name::AbstractString, hard_max::Int)
    raw isa Integer && !(raw isa Bool) || throw(ArgumentError(
        "$(name) must be an integer"))
    value = try
        Int(raw)
    catch
        throw(ArgumentError("$(name) is outside the supported Int range"))
    end
    0 < value <= hard_max || throw(ArgumentError(
        "$(name) must lie in 1:$(hard_max)"))
    return value
end

function ROStratifiedField3DLimits(;
    max_cells::Integer=64,
    max_facets::Integer=8_192,
    max_internal_facets::Integer=2_016,
    max_outputs::Integer=4_096,
    max_exact_basis_points::Integer=6_048,
    max_exact_equations::Integer=50_000_000,
    max_geometry_exact_bit_work::Integer=100_000_000,
    max_geometry_total_work::Integer=100_000_000,
    max_exact_bit_work::Integer=100_000_000,
    max_exact_residual_bits::Integer=1_000_000,
    max_witnesses::Integer=256,
    max_identity_bytes::Integer=64 * 1024 * 1024,
)
    return ROStratifiedField3DLimits(
        _ROSF3_VALIDATED,
        _rosf3_positive_limit(max_cells, "max_cells", _ROSF3_MAX_CELLS),
        _rosf3_positive_limit(max_facets, "max_facets", _ROSF3_MAX_FACETS),
        _rosf3_positive_limit(max_internal_facets,
            "max_internal_facets", _ROSF3_MAX_FACETS),
        _rosf3_positive_limit(max_outputs, "max_outputs", _ROSF3_MAX_OUTPUTS),
        _rosf3_positive_limit(max_exact_basis_points,
            "max_exact_basis_points", 3 * _ROSF3_MAX_FACETS),
        _rosf3_positive_limit(max_exact_equations,
            "max_exact_equations", _ROSF3_MAX_EQUATIONS),
        _rosf3_positive_limit(max_geometry_exact_bit_work,
            "max_geometry_exact_bit_work", _ROSF3_MAX_EXACT_BIT_WORK),
        _rosf3_positive_limit(max_geometry_total_work,
            "max_geometry_total_work", _ROSF3_MAX_EXACT_BIT_WORK),
        _rosf3_positive_limit(max_exact_bit_work,
            "max_exact_bit_work", _ROSF3_MAX_EXACT_BIT_WORK),
        _rosf3_positive_limit(max_exact_residual_bits,
            "max_exact_residual_bits", _ROSF3_MAX_EXACT_RESIDUAL_BITS),
        _rosf3_positive_limit(max_witnesses,
            "max_witnesses", _ROSF3_MAX_WITNESSES),
        _rosf3_positive_limit(max_identity_bytes,
            "max_identity_bytes", _ROSF3_MAX_IDENTITY_BYTES),
    )
end

"""Raised before exact D=3 stratified analysis exceeds a declared bound."""
struct ROStratifiedField3DLimitExceeded <: Exception
    phase::Symbol
    requested::BigInt
    limit::Int
end

function Base.showerror(io::IO, err::ROStratifiedField3DLimitExceeded)
    print(io, "exact D=3 stratified RO-field analysis ", err.phase,
        " requires ", err.requested, ", exceeding limit=", err.limit)
end

@inline function _rosf3_limit(phase::Symbol, requested::Integer, limit::Int)
    amount = BigInt(requested)
    amount <= limit || throw(
        ROStratifiedField3DLimitExceeded(phase, amount, limit))
    return nothing
end

"""Raised when a stored exact D=3 certificate fails self-validation."""
struct ROStratifiedField3DValidationError <: Exception
    reason::String
end

Base.showerror(io::IO, err::ROStratifiedField3DValidationError) =
    print(io, "stored exact D=3 regular-extension certificate is invalid: ",
        err.reason)

"""One bounded exact obstruction witness; public construction is disabled."""
struct ROIntegrabilityObstruction3D
    kind::Symbol
    facet_ids::Vector{Int}
    cell_ids::Vector{Int}
    output_position::Int
    output_index::Int
    exact_residual::String

    function ROIntegrabilityObstruction3D(::_ROSF3ValidatedToken, args...)
        new(args...)
    end
end

"""
Content-bound evidence for one complete explicit-affine D=3 regular extension.

All admitted Float64 geometry and affine labels are interpreted as their exact
dyadic rational values. The artifact separates existence of compatible cell
offsets from continuity of the offsets supplied by the labels. It is always a
regular-limit result and never includes a physical singular branch.
"""
struct RORegularExtensionIntegrabilityCertificate3D
    schema_version::String
    source_complex_identity::String
    output_indices::Vector{Int}
    status::Symbol
    gradient_integrability_status::Symbol
    provided_potential_status::Symbol
    reasons::Vector{Symbol}
    coordinate_semantics::Symbol
    domain_topology::Symbol
    regular_limit_only::Bool
    includes_singular_branch::Bool
    checked_cell_count::Int
    checked_facet_count::Int
    checked_internal_facet_count::Int
    output_count::Int
    checked_exact_basis_point_count::Int
    checked_tangential_equation_count::Int
    checked_offset_equation_count::Int
    checked_cycle_equation_count::Int
    dual_graph_component_count::Int
    dual_graph_edge_count::Int
    dual_graph_cycle_rank::Int
    tangential_obstruction_count::Int
    cycle_obstruction_count::Int
    provided_offset_obstruction_count::Int
    singular_stratum_count::Int
    witnesses::Vector{ROIntegrabilityObstruction3D}
    witnesses_truncated::Bool
    analysis_limits::ROStratifiedField3DLimits
    evidence_scope::Symbol
    arbitrary_real_certified::Bool
    higher_dimension_certified::Bool
    holed_domain_cohomology_certified::Bool
    chemistry_extraction_certified::Bool
    canonical_payload::String
    canonical_identity::String

    function RORegularExtensionIntegrabilityCertificate3D(
        ::_ROSF3ValidatedToken, args...)
        new(args...)
    end
end

function _rosf3_copy_limits(limits::ROStratifiedField3DLimits,
    cancel_check=() -> nothing)
    _ro3_checkpoint(cancel_check)
    return ROStratifiedField3DLimits(
        max_cells=limits.max_cells,
        max_facets=limits.max_facets,
        max_internal_facets=limits.max_internal_facets,
        max_outputs=limits.max_outputs,
        max_exact_basis_points=limits.max_exact_basis_points,
        max_exact_equations=limits.max_exact_equations,
        max_geometry_exact_bit_work=limits.max_geometry_exact_bit_work,
        max_geometry_total_work=limits.max_geometry_total_work,
        max_exact_bit_work=limits.max_exact_bit_work,
        max_exact_residual_bits=limits.max_exact_residual_bits,
        max_witnesses=limits.max_witnesses,
        max_identity_bytes=limits.max_identity_bytes,
    )
end

function _rosf3_limits_payload(limits::ROStratifiedField3DLimits,
    cancel_check=() -> nothing)
    names = fieldnames(ROStratifiedField3DLimits)
    values = ntuple(length(names)) do index
        _ro3_checkpoint(cancel_check)
        getfield(limits, names[index])
    end
    return NamedTuple{names}(values)
end

function _rosf3_witness_payload(witness::ROIntegrabilityObstruction3D,
    cancel_check=() -> nothing)
    _ro3_checkpoint(cancel_check)
    return (
        kind=String(witness.kind),
        facet_ids=_ro3_cancellable_copy(
            witness.facet_ids, cancel_check),
        cell_ids=_ro3_cancellable_copy(
            witness.cell_ids, cancel_check),
        output_position=witness.output_position,
        output_index=witness.output_index,
        exact_residual=witness.exact_residual,
    )
end

function _rosf3_certificate_payload(certificate,
    cancel_check=() -> nothing)
    _ro3_checkpoint(cancel_check)
    witness_payloads = Any[]
    sizehint!(witness_payloads, length(certificate.witnesses))
    for witness in certificate.witnesses
        _ro3_checkpoint(cancel_check)
        push!(witness_payloads,
            _rosf3_witness_payload(witness, cancel_check))
    end
    reasons = String[]
    sizehint!(reasons, length(certificate.reasons))
    for reason in certificate.reasons
        _ro3_checkpoint(cancel_check)
        push!(reasons, String(reason))
    end
    return (
        schema_version=certificate.schema_version,
        source_complex_identity=certificate.source_complex_identity,
        output_indices=_ro3_cancellable_copy(
            certificate.output_indices, cancel_check),
        status=String(certificate.status),
        gradient_integrability_status=
            String(certificate.gradient_integrability_status),
        provided_potential_status=
            String(certificate.provided_potential_status),
        reasons=reasons,
        coordinate_semantics=String(certificate.coordinate_semantics),
        domain_topology=String(certificate.domain_topology),
        regular_limit_only=certificate.regular_limit_only,
        includes_singular_branch=certificate.includes_singular_branch,
        checked_cell_count=certificate.checked_cell_count,
        checked_facet_count=certificate.checked_facet_count,
        checked_internal_facet_count=
            certificate.checked_internal_facet_count,
        output_count=certificate.output_count,
        checked_exact_basis_point_count=
            certificate.checked_exact_basis_point_count,
        checked_tangential_equation_count=
            certificate.checked_tangential_equation_count,
        checked_offset_equation_count=
            certificate.checked_offset_equation_count,
        checked_cycle_equation_count=
            certificate.checked_cycle_equation_count,
        dual_graph_component_count=certificate.dual_graph_component_count,
        dual_graph_edge_count=certificate.dual_graph_edge_count,
        dual_graph_cycle_rank=certificate.dual_graph_cycle_rank,
        tangential_obstruction_count=
            certificate.tangential_obstruction_count,
        cycle_obstruction_count=certificate.cycle_obstruction_count,
        provided_offset_obstruction_count=
            certificate.provided_offset_obstruction_count,
        singular_stratum_count=certificate.singular_stratum_count,
        witnesses=witness_payloads,
        witnesses_truncated=certificate.witnesses_truncated,
        analysis_limits=_rosf3_limits_payload(
            certificate.analysis_limits, cancel_check),
        evidence_scope=String(certificate.evidence_scope),
        arbitrary_real_certified=certificate.arbitrary_real_certified,
        higher_dimension_certified=certificate.higher_dimension_certified,
        holed_domain_cohomology_certified=
            certificate.holed_domain_cohomology_certified,
        chemistry_extraction_certified=
            certificate.chemistry_extraction_certified,
    )
end

function _rosf3_identity_reservation(output_indices, reasons, witnesses,
    limits, cancel_check=() -> nothing)
    _ro3_checkpoint(cancel_check)
    bytes = BigInt(16_384) + BigInt(32) * length(output_indices) +
        BigInt(64) * length(reasons)
    for witness in witnesses
        _ro3_checkpoint(cancel_check)
        bytes += 512 + BigInt(32) * (
            length(witness.facet_ids) + length(witness.cell_ids)) +
            ncodeunits(witness.exact_residual)
    end
    _rosf3_limit(:identity_reservation, bytes, limits.max_identity_bytes)
    _ro3_checkpoint(cancel_check)
    return nothing
end

function _rosf3_finalize_certificate(data, limits, cancel_check)
    _ro3_checkpoint(cancel_check)
    reasons = sort!(unique(Symbol.(data.reasons)); by=string)
    _ro3_checkpoint(cancel_check)
    witnesses = _ro3_cancellable_copy(data.witnesses, cancel_check)
    _rosf3_identity_reservation(
        data.output_indices, reasons, witnesses, limits, cancel_check)
    copied_limits = _rosf3_copy_limits(limits, cancel_check)
    provisional = RORegularExtensionIntegrabilityCertificate3D(
        _ROSF3_VALIDATED,
        RO_REGULAR_EXTENSION_INTEGRABILITY_3D_VERSION,
        String(data.source_complex_identity),
        _ro3_cancellable_copy(data.output_indices, cancel_check),
        data.status,
        data.gradient_integrability_status,
        data.provided_potential_status,
        reasons,
        RO_REGULAR_EXTENSION_INTEGRABILITY_3D_COORDINATES,
        RO_REGULAR_EXTENSION_INTEGRABILITY_3D_DOMAIN_TOPOLOGY,
        true,
        false,
        data.checked_cell_count,
        data.checked_facet_count,
        data.checked_internal_facet_count,
        length(data.output_indices),
        data.checked_exact_basis_point_count,
        data.checked_tangential_equation_count,
        data.checked_offset_equation_count,
        data.checked_cycle_equation_count,
        data.dual_graph_component_count,
        data.dual_graph_edge_count,
        data.dual_graph_cycle_rank,
        data.tangential_obstruction_count,
        data.cycle_obstruction_count,
        data.provided_offset_obstruction_count,
        data.singular_stratum_count,
        witnesses,
        data.witnesses_truncated,
        copied_limits,
        RO_REGULAR_EXTENSION_INTEGRABILITY_3D_SCOPE,
        false,
        false,
        false,
        false,
        "",
        "",
    )
    _ro3_checkpoint(cancel_check)
    payload_data = _rosf3_certificate_payload(provisional, cancel_check)
    _ro3_checkpoint(cancel_check)
    payload = String(JSON3.write(payload_data))
    _ro3_checkpoint(cancel_check)
    _rosf3_limit(:identity_bytes, ncodeunits(payload), limits.max_identity_bytes)
    _ro3_checkpoint(cancel_check)
    identity = "sha256:" * bytes2hex(SHA.sha256(codeunits(payload)))
    _ro3_checkpoint(cancel_check)
    return RORegularExtensionIntegrabilityCertificate3D(
        _ROSF3_VALIDATED,
        provisional.schema_version,
        provisional.source_complex_identity,
        provisional.output_indices,
        provisional.status,
        provisional.gradient_integrability_status,
        provisional.provided_potential_status,
        provisional.reasons,
        provisional.coordinate_semantics,
        provisional.domain_topology,
        provisional.regular_limit_only,
        provisional.includes_singular_branch,
        provisional.checked_cell_count,
        provisional.checked_facet_count,
        provisional.checked_internal_facet_count,
        provisional.output_count,
        provisional.checked_exact_basis_point_count,
        provisional.checked_tangential_equation_count,
        provisional.checked_offset_equation_count,
        provisional.checked_cycle_equation_count,
        provisional.dual_graph_component_count,
        provisional.dual_graph_edge_count,
        provisional.dual_graph_cycle_rank,
        provisional.tangential_obstruction_count,
        provisional.cycle_obstruction_count,
        provisional.provided_offset_obstruction_count,
        provisional.singular_stratum_count,
        provisional.witnesses,
        provisional.witnesses_truncated,
        provisional.analysis_limits,
        provisional.evidence_scope,
        provisional.arbitrary_real_certified,
        provisional.higher_dimension_certified,
        provisional.holed_domain_cohomology_certified,
        provisional.chemistry_extraction_certified,
        payload,
        identity,
    )
end

function _rosf3_output_indices(raw, limits::ROStratifiedField3DLimits,
    cancel_check=() -> nothing)
    _ro3_checkpoint(cancel_check)
    (raw isa AbstractVector || raw isa Tuple) || throw(ArgumentError(
        "output_indices must be an indexable finite collection"))
    count = length(raw)
    count > 0 || throw(ArgumentError(
        "output_indices must contain at least one output"))
    _rosf3_limit(:outputs, count, limits.max_outputs)
    count <= _ROSF3_MAX_OUTPUTS || throw(ArgumentError(
        "output_indices exceeds the absolute hard maximum"))
    outputs = Vector{Int}(undef, count)
    seen = Set{Int}()
    for (target, source) in enumerate(eachindex(raw))
        _ro3_checkpoint(cancel_check)
        value = raw[source]
        value isa Integer && !(value isa Bool) || throw(ArgumentError(
            "output_indices must contain positive non-Boolean integers"))
        converted = try
            Int(value)
        catch
            throw(ArgumentError("an output index is outside the Int range"))
        end
        converted > 0 || throw(ArgumentError(
            "output_indices must contain positive indices"))
        converted in seen && throw(ArgumentError(
            "output_indices must be unique and ordered"))
        push!(seen, converted)
        outputs[target] = converted
    end
    return outputs
end

function _rosf3_geometry_limits(complex::ROCellComplex3D,
    limits::ROStratifiedField3DLimits)
    original = complex.construction_limits
    return ROCellComplex3DLimits(
        max_cell_specs=min(original.max_cell_specs, limits.max_cells),
        max_halfspaces_per_cell=original.max_halfspaces_per_cell,
        max_pair_checks=original.max_pair_checks,
        max_facets=min(original.max_facets, limits.max_facets),
        max_ridges=original.max_ridges,
        max_vertices=original.max_vertices,
        max_strata=original.max_strata,
        max_labels=original.max_labels,
        max_source_regime_ids=original.max_source_regime_ids,
        max_fixed_background=original.max_fixed_background,
        max_outputs=min(original.max_outputs, limits.max_outputs),
        max_total_halfspaces=original.max_total_halfspaces,
        max_facet_edges=original.max_facet_edges,
        max_facet_ridge_scans=original.max_facet_ridge_scans,
        max_exact_bit_work=min(original.max_exact_bit_work,
            limits.max_geometry_exact_bit_work),
        max_annotation_work=original.max_annotation_work,
        max_identity_bytes=original.max_identity_bytes,
        max_total_work=min(original.max_total_work,
            limits.max_geometry_total_work),
    )
end

function _rosf3_geometry_preflight(complex, limits, cancel_check)
    _rosf3_limit(:cell_specs, length(complex.source_specs), limits.max_cells)
    geometry_limits = _rosf3_geometry_limits(complex, limits)
    try
        _ro3_preflight_inputs(
            complex.domain,
            complex.source_specs,
            complex.interface_annotations,
            complex.tolerances,
            geometry_limits,
            cancel_check,
        )
    catch err
        if err isa ROCellComplex3DLimitExceeded
            throw(ROStratifiedField3DLimitExceeded(
                Symbol("geometry_", String(err.phase)),
                err.requested,
                err.limit,
            ))
        end
        rethrow()
    end
    return nothing
end

@inline function _rosf3_exact_bit_length(value::_RO3Exact)
    return max(
        ndigits(abs(numerator(value)); base=2),
        ndigits(denominator(value); base=2),
    )
end

function _rosf3_exact_token(value::_RO3Exact,
    limits::ROStratifiedField3DLimits)
    bits = _rosf3_exact_bit_length(value)
    _rosf3_limit(:exact_residual_bits, bits,
        limits.max_exact_residual_bits)
    return _ro3_exact_token(value)
end

function _rosf3_record_witness!(
    witnesses::Vector{ROIntegrabilityObstruction3D},
    truncated::Base.RefValue{Bool},
    kind::Symbol,
    facet_ids,
    cell_ids,
    output_position::Int,
    output_index::Int,
    residual::_RO3Exact,
    limits::ROStratifiedField3DLimits,
)
    if length(witnesses) >= limits.max_witnesses
        truncated[] = true
        return nothing
    end
    exact_residual = _rosf3_exact_token(residual, limits)
    facets = sort!(unique(Int.(facet_ids)))
    cells = sort!(unique(Int.(cell_ids)))
    push!(witnesses, ROIntegrabilityObstruction3D(
        _ROSF3_VALIDATED,
        kind,
        facets,
        cells,
        output_position,
        output_index,
        exact_residual,
    ))
    return nothing
end

struct _ROSF3FacetBasis
    facet_id::Int
    cell_ids::NTuple{2,Int}
    points::NTuple{3,NTuple{3,_RO3Exact}}
end

function _rosf3_exact_affine_basis(points, context, cancel_check)
    by_key = Dict{String,NTuple{3,_RO3Exact}}()
    for point in points
        _ro3_checkpoint(cancel_check)
        by_key[_ro3_exact_point_key(point)] = point
    end
    keys_sorted = collect(keys(by_key))
    _ro3_cancellable_sort!(keys_sorted, cancel_check)
    length(keys_sorted) >= 3 || throw(ROCellComplex3DClosureError(
        "$(context) lacks three exact facet points"))
    anchor = by_key[first(keys_sorted)]
    second = nothing
    third = nothing
    for key in keys_sorted[2:end]
        _ro3_checkpoint(cancel_check)
        candidate = by_key[key]
        if second === nothing
            candidate == anchor || (second = candidate)
            continue
        end
        if _ro3_exact_point_rank([anchor, second, candidate], cancel_check) == 2
            third = candidate
            break
        end
    end
    second === nothing && throw(ROCellComplex3DClosureError(
        "$(context) lacks an exact nonzero tangent"))
    third === nothing && throw(ROCellComplex3DClosureError(
        "$(context) lacks an exact rank-two affine basis"))
    return (anchor, second::NTuple{3,_RO3Exact},
        third::NTuple{3,_RO3Exact})
end

function _rosf3_exact_facet_bases(complex, limits, cancel_check)
    exact_unmerged = _RO3ExactCellWork[]
    sizehint!(exact_unmerged, length(complex.source_specs))
    for spec in complex.source_specs
        _ro3_checkpoint(cancel_check)
        push!(exact_unmerged,
            _ro3_build_exact_cell(spec, complex.domain, cancel_check))
    end
    exact_cells = _ro3_merge_exact_cells(exact_unmerged, cancel_check)
    length(exact_cells) == length(complex.cells) || throw(
        ROCellComplex3DClosureError(
            "exact integrability rebuild changed the cell population"))
    for cell_id in eachindex(exact_cells)
        _ro3_checkpoint(cancel_check)
        exact_cells[cell_id].source_keys == complex.cells[cell_id].source_keys ||
            throw(ROCellComplex3DClosureError(
                "exact integrability rebuild changed cell ordering"))
    end

    exact_pairs = _ro3_exact_pair_certificate(exact_cells, cancel_check)
    facet_by_pair = Dict{Tuple{Int,Int},Int}()
    for facet in complex.facets
        _ro3_checkpoint(cancel_check)
        facet.kind === :internal || continue
        length(facet.incident_cell_ids) == 2 || throw(
            ROCellComplex3DClosureError(
                "an internal facet lacks two incident cells"))
        pair = minmax(facet.incident_cell_ids...)
        haskey(facet_by_pair, pair) && throw(ROCellComplex3DClosureError(
            "one cell pair owns multiple internal facets"))
        facet_by_pair[pair] = facet.id
    end

    bases = _ROSF3FacetBasis[]
    pairs = collect(keys(exact_pairs.pair_results))
    _ro3_cancellable_sort!(pairs, cancel_check)
    for pair in pairs
        _ro3_checkpoint(cancel_check)
        result = exact_pairs.pair_results[pair]
        result.dimension == 2 || continue
        facet_id = get(facet_by_pair, pair, 0)
        iszero(facet_id) && throw(ROCellComplex3DClosureError(
            "an exact two-dimensional cell intersection lacks an internal facet"))
        result.support_plane_key === nothing && throw(
            ROCellComplex3DClosureError(
                "an exact internal facet lacks a support-plane receipt"))
        basis = _rosf3_exact_affine_basis(result.points,
            "exact internal facet $(facet_id)", cancel_check)
        push!(bases, _ROSF3FacetBasis(facet_id, pair, basis))
    end
    length(bases) == length(facet_by_pair) || throw(
        ROCellComplex3DClosureError(
            "Float64 and exact internal-facet populations disagree"))
    _ro3_cancellable_sort!(bases, cancel_check; by=basis -> basis.facet_id)
    _rosf3_limit(:exact_basis_points, BigInt(3) * length(bases),
        limits.max_exact_basis_points)
    return bases
end

struct _ROSF3DualTopology
    component_count::Int
    roots::Vector{Int}
    parent::Vector{Int}
    parent_edge::Vector{Int}
    traversal_order::Vector{Int}
    tree_edge::BitVector
    cycle_edges::Vector{Int}
end

function _rosf3_dual_topology(cell_count::Int,
    bases::AbstractVector{_ROSF3FacetBasis}, cancel_check=() -> nothing)
    cell_count > 0 || throw(ArgumentError(
        "a dual topology requires at least one cell"))
    adjacency = [Tuple{Int,Int}[] for _ in 1:cell_count]
    for (edge_index, basis) in enumerate(bases)
        _ro3_checkpoint(cancel_check)
        left, right = basis.cell_ids
        1 <= left <= cell_count && 1 <= right <= cell_count && left != right ||
            throw(ROCellComplex3DClosureError(
                "an internal facet has invalid dual incidence"))
        push!(adjacency[left], (right, edge_index))
        push!(adjacency[right], (left, edge_index))
    end
    for neighbors in adjacency
        _ro3_cancellable_sort!(neighbors, cancel_check;
            by=item -> (item[1], bases[item[2]].facet_id))
    end

    parent = zeros(Int, cell_count)
    parent_edge = zeros(Int, cell_count)
    visited = falses(cell_count)
    tree_edge = falses(length(bases))
    roots = Int[]
    order = Int[]
    queue = Int[]
    for root in 1:cell_count
        _ro3_checkpoint(cancel_check)
        visited[root] && continue
        push!(roots, root)
        empty!(queue)
        push!(queue, root)
        visited[root] = true
        cursor = 1
        while cursor <= length(queue)
            _ro3_checkpoint(cancel_check)
            cell = queue[cursor]
            cursor += 1
            push!(order, cell)
            for (neighbor, edge_index) in adjacency[cell]
                _ro3_checkpoint(cancel_check)
                visited[neighbor] && continue
                visited[neighbor] = true
                parent[neighbor] = cell
                parent_edge[neighbor] = edge_index
                tree_edge[edge_index] = true
                push!(queue, neighbor)
            end
        end
    end
    cycle_edges = findall(!, tree_edge)
    return _ROSF3DualTopology(length(roots), roots, parent, parent_edge,
        order, tree_edge, cycle_edges)
end

function _rosf3_solve_dual_offsets(topology::_ROSF3DualTopology,
    bases::AbstractVector{_ROSF3FacetBasis},
    deltas::AbstractVector{_RO3Exact}, cancel_check=() -> nothing)
    length(deltas) == length(bases) || throw(DimensionMismatch(
        "one exact jump is required for every dual edge"))
    offsets = zeros(_RO3Exact, length(topology.parent))
    for cell in topology.traversal_order
        _ro3_checkpoint(cancel_check)
        edge_index = topology.parent_edge[cell]
        iszero(edge_index) && continue
        parent = topology.parent[cell]
        left, right = bases[edge_index].cell_ids
        if parent == left && cell == right
            offsets[cell] = offsets[parent] + deltas[edge_index]
        elseif parent == right && cell == left
            offsets[cell] = offsets[parent] - deltas[edge_index]
        else
            throw(ROCellComplex3DClosureError(
                "dual spanning-tree incidence is inconsistent"))
        end
    end
    residuals = Tuple{Int,_RO3Exact}[]
    for edge_index in topology.cycle_edges
        _ro3_checkpoint(cancel_check)
        left, right = bases[edge_index].cell_ids
        residual = offsets[right] - offsets[left] - deltas[edge_index]
        iszero(residual) || push!(residuals, (edge_index, residual))
    end
    return offsets, residuals
end

function _rosf3_fundamental_cycle(topology, bases, edge_index)
    left, right = bases[edge_index].cell_ids
    left_nodes = Int[]
    left_edges = Int[]
    node = left
    while !iszero(node)
        push!(left_nodes, node)
        edge = topology.parent_edge[node]
        iszero(edge) || push!(left_edges, edge)
        node = topology.parent[node]
    end
    left_position = Dict(value => index for (index, value) in
        enumerate(left_nodes))
    right_nodes = Int[]
    right_edges = Int[]
    node = right
    while !(node in keys(left_position))
        push!(right_nodes, node)
        edge = topology.parent_edge[node]
        iszero(edge) && throw(ROCellComplex3DClosureError(
            "a cotree edge crosses dual components"))
        push!(right_edges, edge)
        node = topology.parent[node]
    end
    push!(right_nodes, node)
    left_stop = left_position[node]
    cycle_edges = vcat(left_edges[1:max(0, left_stop - 1)],
        right_edges, [edge_index])
    facet_ids = sort!(unique([bases[index].facet_id for index in cycle_edges]))
    cell_ids = sort!(unique(vcat(left_nodes[1:left_stop], right_nodes)))
    return facet_ids, cell_ids
end

@inline function _rosf3_exact_point_subtract(left, right)
    return (left[1] - right[1], left[2] - right[2],
        left[3] - right[3])
end

function _rosf3_exact_row_difference_dot(left, right, row::Int, vector)
    result = zero(_RO3Exact)
    for column in 1:3
        result += (left[row, column] - right[row, column]) * vector[column]
    end
    return result
end

function _rosf3_label_bit_length(label::ROAffineLabel3D,
    cancel_check=() -> nothing)
    bits = 1
    for value in label.reaction_order_matrix
        _ro3_checkpoint(cancel_check)
        bits = max(bits, _ro3_dyadic_bit_length(value))
    end
    for value in label.output_offset
        _ro3_checkpoint(cancel_check)
        bits = max(bits, _ro3_dyadic_bit_length(value))
    end
    return bits
end

function _rosf3_arithmetic_preflight(complex, bases, output_count, limits,
    cancel_check)
    label_bits = Vector{Int}(undef, length(complex.cells))
    maximum_label_bits = 1
    label_scalar_count = BigInt(0)
    for cell in complex.cells
        _ro3_checkpoint(cancel_check)
        label = only(cell.labels)
        label_bits[cell.id] = _rosf3_label_bit_length(label, cancel_check)
        maximum_label_bits = max(maximum_label_bits, label_bits[cell.id])
        label_scalar_count += length(label.reaction_order_matrix) +
            length(label.output_offset)
    end
    _rosf3_limit(:exact_label_scalars, label_scalar_count,
        limits.max_exact_equations)

    bit_work = BigInt(0)
    for basis in bases
        _ro3_checkpoint(cancel_check)
        point_bits = 1
        for point in basis.points, value in point
            _ro3_checkpoint(cancel_check)
            point_bits = max(point_bits, _rosf3_exact_bit_length(value))
        end
        left, right = basis.cell_ids
        operand_bits = BigInt(label_bits[left] + label_bits[right] +
            point_bits + 4)
        # Per output: two tangent equations, one anchor jump, and one supplied
        # offset comparison. The factor accounts for exact differences, three
        # products, reductions, and Rational{BigInt} normalization.
        bit_work += BigInt(64) * output_count * operand_bits^2
    end
    graph_work = BigInt(32) * output_count *
        (length(complex.cells) + length(bases) + 1) *
        BigInt(maximum_label_bits + 4)^2
    bit_work += graph_work
    _rosf3_limit(:exact_bit_work, bit_work, limits.max_exact_bit_work)
    return label_bits
end

function _rosf3_exact_labels(complex, cancel_check)
    jacobians = Vector{Matrix{_RO3Exact}}(undef, length(complex.cells))
    offsets = Vector{Vector{_RO3Exact}}(undef, length(complex.cells))
    for cell in complex.cells
        _ro3_checkpoint(cancel_check)
        label = only(cell.labels)
        jacobian = Matrix{_RO3Exact}(undef,
            size(label.reaction_order_matrix))
        for index in eachindex(label.reaction_order_matrix)
            _ro3_checkpoint(cancel_check)
            jacobian[index] = _ro3_exact(label.reaction_order_matrix[index])
        end
        offset = Vector{_RO3Exact}(undef, length(label.output_offset))
        for index in eachindex(label.output_offset)
            _ro3_checkpoint(cancel_check)
            offset[index] = _ro3_exact(label.output_offset[index])
        end
        jacobians[cell.id] = jacobian
        offsets[cell.id] = offset
    end
    return jacobians, offsets
end

function _rosf3_unknown_data(complex, output_indices, reasons;
    internal_count=0,
    basis_point_count=0,
    component_count=0,
    edge_count=0,
    cycle_rank=0,
)
    return (
        source_complex_identity=complex.canonical_identity,
        output_indices=output_indices,
        status=:unknown,
        gradient_integrability_status=:unknown,
        provided_potential_status=:unknown,
        reasons=Symbol.(reasons),
        checked_cell_count=length(complex.cells),
        checked_facet_count=length(complex.facets),
        checked_internal_facet_count=internal_count,
        checked_exact_basis_point_count=basis_point_count,
        checked_tangential_equation_count=0,
        checked_offset_equation_count=0,
        checked_cycle_equation_count=0,
        dual_graph_component_count=component_count,
        dual_graph_edge_count=edge_count,
        dual_graph_cycle_rank=cycle_rank,
        tangential_obstruction_count=0,
        cycle_obstruction_count=0,
        provided_offset_obstruction_count=0,
        singular_stratum_count=count(
            stratum -> stratum.kind === :singular, complex.strata),
        witnesses=ROIntegrabilityObstruction3D[],
        witnesses_truncated=false,
    )
end

"""
    certify_ro_regular_extension_integrability_3d(
        complex, output_indices; limits=..., cancel_check=...)

Rebuild and validate one explicit-affine D=3 complex, reinterpret every
admitted Float64 geometry/label value as its exact dyadic rational, and check:

1. exact tangential compatibility of adjacent cell Jacobians on a three-point
   affine basis of every internal facet;
2. exact consistency of the induced offset jumps around every deterministic
   dual-graph cotree cycle; and
3. exact continuity of the affine offsets supplied by the cell labels.

The positive result is scoped to the complete contractible box and its regular
extension. Singular strata may be present, but no singular branch is selected.
Malformed identity, resource exhaustion, cancellation, or stale complex state
never returns a scientific certificate.
"""
function certify_ro_regular_extension_integrability_3d(
    complex::ROCellComplex3D,
    raw_output_indices;
    limits::ROStratifiedField3DLimits=ROStratifiedField3DLimits(),
    cancel_check=() -> nothing,
)
    _ro3_checkpoint(cancel_check)
    output_indices = _rosf3_output_indices(
        raw_output_indices, limits, cancel_check)
    cell_count = length(complex.cells)
    facet_count = length(complex.facets)
    _rosf3_limit(:cells, cell_count, limits.max_cells)
    _rosf3_limit(:facets, facet_count, limits.max_facets)
    cell_count > 0 || throw(ArgumentError(
        "a D=3 regular-extension analysis requires at least one cell"))

    internal_count = 0
    for facet in complex.facets
        _ro3_checkpoint(cancel_check)
        facet.kind === :internal && (internal_count += 1)
    end
    _rosf3_limit(:internal_facets, internal_count,
        limits.max_internal_facets)
    _rosf3_limit(:exact_basis_points, BigInt(3) * internal_count,
        limits.max_exact_basis_points)
    equation_upper = BigInt(4) * internal_count * length(output_indices)
    _rosf3_limit(:exact_equations, equation_upper,
        limits.max_exact_equations)

    # Output identity is checked before any CDD reconstruction. The complex
    # currently stores only row order, so callers must bind explicit indices.
    for cell in complex.cells
        _ro3_checkpoint(cancel_check)
        isempty(cell.labels) && continue
        rows = size(first(cell.labels).reaction_order_matrix, 1)
        rows == length(output_indices) || throw(DimensionMismatch(
            "output_indices length must equal every 3D affine-label row count"))
    end

    _rosf3_geometry_preflight(complex, limits, cancel_check)
    validate_ro_cell_complex_3d(complex; cancel_check=cancel_check)

    eligibility_reasons = Symbol[]
    complex.certificate.publishable ||
        push!(eligibility_reasons, :geometry_not_publishable)
    complex.has_ambiguity && push!(eligibility_reasons, :ambiguous_complex)
    for cell in complex.cells
        _ro3_checkpoint(cancel_check)
        cell.set_valued && push!(eligibility_reasons, :set_valued_cell)
        length(cell.labels) == 1 ||
            push!(eligibility_reasons, :invalid_cell_label_count)
        if !isempty(cell.labels)
            label = first(cell.labels)
            size(label.reaction_order_matrix) ==
                (length(output_indices), 3) ||
                push!(eligibility_reasons, :invalid_label_matrix_shape)
            length(label.output_offset) == length(output_indices) ||
                push!(eligibility_reasons, :invalid_label_offset_shape)
        end
    end
    if !isempty(eligibility_reasons)
        return _rosf3_finalize_certificate(
            _rosf3_unknown_data(complex, output_indices,
                eligibility_reasons; internal_count=internal_count),
            limits,
            cancel_check,
        )
    end

    bases = _rosf3_exact_facet_bases(
        complex, limits, cancel_check)
    topology = _rosf3_dual_topology(cell_count, bases, cancel_check)
    cycle_rank = length(bases) - cell_count + topology.component_count
    cycle_rank >= 0 || throw(ROCellComplex3DClosureError(
        "the exact internal-facet dual graph has negative cycle rank"))
    if topology.component_count != 1
        return _rosf3_finalize_certificate(
            _rosf3_unknown_data(complex, output_indices,
                [:disconnected_internal_facet_dual_graph];
                internal_count=length(bases),
                basis_point_count=3 * length(bases),
                component_count=topology.component_count,
                edge_count=length(bases),
                cycle_rank=cycle_rank),
            limits,
            cancel_check,
        )
    end

    _rosf3_arithmetic_preflight(
        complex, bases, length(output_indices), limits, cancel_check)
    jacobians, offsets = _rosf3_exact_labels(complex, cancel_check)

    witnesses = ROIntegrabilityObstruction3D[]
    witnesses_truncated = Ref(false)
    tangential_obstructions = 0
    cycle_obstructions = 0
    provided_offset_obstructions = 0
    checked_tangential = 0
    checked_offsets = 0
    checked_cycles = 0

    for output_position in eachindex(output_indices)
        _ro3_checkpoint(cancel_check)
        output_index = output_indices[output_position]
        deltas = Vector{_RO3Exact}(undef, length(bases))
        output_has_tangential_obstruction = false
        for (edge_index, basis) in enumerate(bases)
            _ro3_checkpoint(cancel_check)
            left, right = basis.cell_ids
            anchor, second, third = basis.points
            first_tangent = _rosf3_exact_point_subtract(second, anchor)
            second_tangent = _rosf3_exact_point_subtract(third, anchor)
            first_residual = _rosf3_exact_row_difference_dot(
                jacobians[left], jacobians[right], output_position,
                first_tangent)
            second_residual = _rosf3_exact_row_difference_dot(
                jacobians[left], jacobians[right], output_position,
                second_tangent)
            checked_tangential += 2
            for residual in (first_residual, second_residual)
                iszero(residual) && continue
                output_has_tangential_obstruction = true
                tangential_obstructions += 1
                _rosf3_record_witness!(witnesses, witnesses_truncated,
                    :exact_tangential_mismatch,
                    [basis.facet_id], collect(basis.cell_ids),
                    output_position, output_index, residual, limits)
            end

            delta = _rosf3_exact_row_difference_dot(
                jacobians[left], jacobians[right], output_position, anchor)
            deltas[edge_index] = delta
            supplied_residual = offsets[right][output_position] -
                offsets[left][output_position] - delta
            checked_offsets += 1
            if !iszero(supplied_residual)
                provided_offset_obstructions += 1
                _rosf3_record_witness!(witnesses, witnesses_truncated,
                    :exact_provided_offset_mismatch,
                    [basis.facet_id], collect(basis.cell_ids),
                    output_position, output_index, supplied_residual, limits)
            end
        end

        output_has_tangential_obstruction && continue
        _, cycle_residuals = _rosf3_solve_dual_offsets(
            topology, bases, deltas, cancel_check)
        checked_cycles += length(topology.cycle_edges)
        for (edge_index, residual) in cycle_residuals
            _ro3_checkpoint(cancel_check)
            cycle_obstructions += 1
            facet_ids, cell_ids = _rosf3_fundamental_cycle(
                topology, bases, edge_index)
            _rosf3_record_witness!(witnesses, witnesses_truncated,
                :exact_dual_cycle_mismatch,
                facet_ids, cell_ids, output_position, output_index,
                residual, limits)
        end
    end
    _ro3_checkpoint(cancel_check)

    reasons = Symbol[]
    gradient_status = if tangential_obstructions > 0
        push!(reasons, :exact_tangential_obstruction)
        :exact_dyadic_tangential_obstruction
    elseif cycle_obstructions > 0
        push!(reasons, :exact_dual_cycle_obstruction)
        :exact_dyadic_cycle_obstruction
    else
        :exact_dyadic_global_integrable
    end
    provided_status = if tangential_obstructions > 0 ||
        provided_offset_obstructions > 0
        provided_offset_obstructions > 0 &&
            push!(reasons, :exact_provided_offset_obstruction)
        :exact_dyadic_discontinuous
    else
        :exact_dyadic_continuous
    end
    status = gradient_status === :exact_dyadic_global_integrable &&
        provided_status === :exact_dyadic_continuous ?
        :regular_cell_extension_integrable :
        :regular_cell_extension_not_integrable

    data = (
        source_complex_identity=complex.canonical_identity,
        output_indices=output_indices,
        status=status,
        gradient_integrability_status=gradient_status,
        provided_potential_status=provided_status,
        reasons=reasons,
        checked_cell_count=cell_count,
        checked_facet_count=facet_count,
        checked_internal_facet_count=length(bases),
        checked_exact_basis_point_count=3 * length(bases),
        checked_tangential_equation_count=checked_tangential,
        checked_offset_equation_count=checked_offsets,
        checked_cycle_equation_count=checked_cycles,
        dual_graph_component_count=topology.component_count,
        dual_graph_edge_count=length(bases),
        dual_graph_cycle_rank=cycle_rank,
        tangential_obstruction_count=tangential_obstructions,
        cycle_obstruction_count=cycle_obstructions,
        provided_offset_obstruction_count=provided_offset_obstructions,
        singular_stratum_count=count(
            stratum -> stratum.kind === :singular, complex.strata),
        witnesses=witnesses,
        witnesses_truncated=witnesses_truncated[],
    )
    return _rosf3_finalize_certificate(data, limits, cancel_check)
end

function _rosf3_validation_failure(reason)
    throw(ROStratifiedField3DValidationError(String(reason)))
end

"""Recompute and validate a content-bound exact D=3 certificate."""
function validate_ro_regular_extension_integrability_certificate_3d(
    complex::ROCellComplex3D,
    certificate::RORegularExtensionIntegrabilityCertificate3D;
    cancel_check=() -> nothing,
)
    _ro3_checkpoint(cancel_check)
    certificate.schema_version ==
        RO_REGULAR_EXTENSION_INTEGRABILITY_3D_VERSION ||
        _rosf3_validation_failure("schema version mismatch")
    certificate.evidence_scope ==
        RO_REGULAR_EXTENSION_INTEGRABILITY_3D_SCOPE ||
        _rosf3_validation_failure("evidence scope mismatch")
    certificate.coordinate_semantics ==
        RO_REGULAR_EXTENSION_INTEGRABILITY_3D_COORDINATES ||
        _rosf3_validation_failure("coordinate semantics mismatch")
    certificate.domain_topology ==
        RO_REGULAR_EXTENSION_INTEGRABILITY_3D_DOMAIN_TOPOLOGY ||
        _rosf3_validation_failure("domain topology mismatch")
    certificate.regular_limit_only ||
        _rosf3_validation_failure("regular-limit boundary was weakened")
    certificate.includes_singular_branch &&
        _rosf3_validation_failure("a singular branch was overclaimed")
    certificate.arbitrary_real_certified &&
        _rosf3_validation_failure("arbitrary-real evidence was overclaimed")
    certificate.higher_dimension_certified &&
        _rosf3_validation_failure("higher-dimensional evidence was overclaimed")
    certificate.holed_domain_cohomology_certified &&
        _rosf3_validation_failure("holed-domain evidence was overclaimed")
    certificate.chemistry_extraction_certified &&
        _rosf3_validation_failure("chemistry extraction was overclaimed")
    certificate.source_complex_identity == complex.canonical_identity ||
        _rosf3_validation_failure("source complex identity mismatch")

    _rosf3_output_indices(
        certificate.output_indices, certificate.analysis_limits,
        cancel_check) ==
        certificate.output_indices ||
        _rosf3_validation_failure("output identity is noncanonical")
    length(certificate.witnesses) <=
        certificate.analysis_limits.max_witnesses ||
        _rosf3_validation_failure("witness population exceeds its limit")
    all(value -> value >= 0, (
        certificate.checked_cell_count,
        certificate.checked_facet_count,
        certificate.checked_internal_facet_count,
        certificate.checked_exact_basis_point_count,
        certificate.checked_tangential_equation_count,
        certificate.checked_offset_equation_count,
        certificate.checked_cycle_equation_count,
        certificate.dual_graph_component_count,
        certificate.dual_graph_edge_count,
        certificate.dual_graph_cycle_rank,
        certificate.tangential_obstruction_count,
        certificate.cycle_obstruction_count,
        certificate.provided_offset_obstruction_count,
        certificate.singular_stratum_count,
    )) || _rosf3_validation_failure("a stored count is negative")
    certificate.output_count == length(certificate.output_indices) ||
        _rosf3_validation_failure("output count disagrees with output identity")

    _rosf3_identity_reservation(certificate.output_indices,
        certificate.reasons, certificate.witnesses,
        certificate.analysis_limits, cancel_check)
    _ro3_checkpoint(cancel_check)
    payload_data = _rosf3_certificate_payload(certificate, cancel_check)
    _ro3_checkpoint(cancel_check)
    payload = String(JSON3.write(payload_data))
    _ro3_checkpoint(cancel_check)
    _rosf3_limit(:identity_bytes, ncodeunits(payload),
        certificate.analysis_limits.max_identity_bytes)
    payload == certificate.canonical_payload ||
        _rosf3_validation_failure("canonical payload is stale")
    _ro3_checkpoint(cancel_check)
    identity = "sha256:" * bytes2hex(SHA.sha256(codeunits(payload)))
    _ro3_checkpoint(cancel_check)
    identity == certificate.canonical_identity ||
        _rosf3_validation_failure("canonical SHA-256 identity is stale")

    expected = certify_ro_regular_extension_integrability_3d(
        complex,
        certificate.output_indices;
        limits=certificate.analysis_limits,
        cancel_check=cancel_check,
    )
    expected.canonical_payload == certificate.canonical_payload ||
        _rosf3_validation_failure(
            "stored evidence differs from exact recomputation")
    expected.canonical_identity == certificate.canonical_identity ||
        _rosf3_validation_failure(
            "stored identity differs from exact recomputation")
    return true
end
