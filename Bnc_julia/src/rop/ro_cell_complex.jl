import SHA

"""
    ROInputDomain2D

Closed rectangular domain for an exact two-total reaction-order construction.
Coordinates and bounds are log10 values. `fixed_logqK` contains the complete
q/K background; the two swept entries are overwritten by domain coordinates.
The background is stored as an immutable tuple. Public access returns a
detached vector so caller mutation cannot change the scientific inputs attached
to an already-built complex.
"""
struct ROInputDomain2D
    axis_indices::NTuple{2,Int}
    lower_log10::NTuple{2,Float64}
    upper_log10::NTuple{2,Float64}
    fixed_logqK::Tuple{Vararg{Float64}}

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
        return new(axis_indices, lower_log10, upper_log10, Tuple(fixed_logqK))
    end
end

function Base.getproperty(domain::ROInputDomain2D, name::Symbol)
    name === :fixed_logqK && return collect(getfield(domain, :fixed_logqK))
    return getfield(domain, name)
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

const _RO2_COPY_CHUNK_ELEMENTS = 4_096

function _ro2_cancellable_copy(
    values::AbstractArray,
    cancel_check=_NO_CANCEL_CHECK,
)
    cancel_check()
    copied = similar(values)
    for (position, index) in enumerate(eachindex(values, copied))
        (position - 1) % _RO2_COPY_CHUNK_ELEMENTS == 0 && cancel_check()
        copied[index] = values[index]
    end
    cancel_check()
    return copied
end

function _ro2_cancellable_collect(
    values,
    ::Type{T},
    cancel_check=_NO_CANCEL_CHECK,
) where {T}
    cancel_check()
    copied = Vector{T}(undef, length(values))
    for index in eachindex(values)
        (index - firstindex(values)) % _RO2_COPY_CHUNK_ELEMENTS == 0 &&
            cancel_check()
        copied[index - firstindex(values) + 1] = values[index]
    end
    cancel_check()
    return copied
end

function _ro2_copy_domain(domain::ROInputDomain2D,
                          cancel_check=_NO_CANCEL_CHECK)
    fixed_logqK = _ro2_cancellable_collect(
        getfield(domain, :fixed_logqK), Float64, cancel_check)
    return ROInputDomain2D(
        getfield(domain, :axis_indices),
        getfield(domain, :lower_log10),
        getfield(domain, :upper_log10),
        fixed_logqK,
    )
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

function _ro2_copy_label(label::ROAffineLabel2D,
                         cancel_check=_NO_CANCEL_CHECK)
    cancel_check()
    return ROAffineLabel2D(
        _ro2_cancellable_copy(
            getfield(label, :source_regime_ids), cancel_check),
        _ro2_cancellable_copy(
            getfield(label, :reaction_order_matrix), cancel_check),
        _ro2_cancellable_copy(
            getfield(label, :output_offset), cancel_check),
    )
end

function _ro2_copy_cell(cell::ROCell2D,
                        cancel_check=_NO_CANCEL_CHECK)
    cancel_check()
    return ROCell2D(
        getfield(cell, :id),
        _ro2_cancellable_copy(getfield(cell, :vertices), cancel_check),
        getfield(cell, :area),
        _ro2_cancellable_copy(
            getfield(cell, :source_regime_ids), cancel_check),
        [_ro2_copy_label(label, cancel_check) for label in getfield(cell, :labels)],
        getfield(cell, :set_valued),
    )
end

function _ro2_copy_facet(facet::ROFacet2D,
                         cancel_check=_NO_CANCEL_CHECK)
    cancel_check()
    return ROFacet2D(
        getfield(facet, :id),
        getfield(facet, :kind),
        getfield(facet, :endpoints),
        _ro2_cancellable_copy(
            getfield(facet, :incident_cell_ids), cancel_check),
        _ro2_cancellable_copy(
            getfield(facet, :singular_stratum_ids), cancel_check),
        getfield(facet, :normal),
        getfield(facet, :offset),
        getfield(facet, :mixed_sign),
        getfield(facet, :domain_side),
    )
end

function _ro2_copy_stratum(stratum::ROSingularStratum2D,
                           cancel_check=_NO_CANCEL_CHECK)
    cancel_check()
    return ROSingularStratum2D(
        getfield(stratum, :id),
        getfield(stratum, :dimension),
        _ro2_cancellable_copy(getfield(stratum, :vertices), cancel_check),
        _ro2_cancellable_copy(
            getfield(stratum, :source_regime_ids), cancel_check),
        _ro2_cancellable_copy(getfield(stratum, :nullities), cancel_check),
        _ro2_cancellable_copy(getfield(stratum, :reasons), cancel_check),
    )
end

"""Hard construction limits for the demonstration-scale exact 2D builder."""
struct ROCellComplexBuildLimits
    max_candidate_regimes::Int
    max_enumeration_work::Int
    max_cells::Int
    max_singular_strata::Int
    max_pair_checks::Int
    max_facets::Int
    max_outputs::Int
    max_matrix_elements::Int
    max_payload_elements::Int
    max_finalization_work::Int
end

function ROCellComplexBuildLimits(;
    max_candidate_regimes::Integer=10_000,
    max_enumeration_work::Integer=16_777_216,
    max_cells::Integer=10_000,
    max_singular_strata::Integer=10_000,
    max_pair_checks::Integer=1_000_000,
    max_facets::Integer=100_000,
    max_outputs::Integer=4_096,
    max_matrix_elements::Integer=1_048_576,
    max_payload_elements::Integer=4_194_304,
    max_finalization_work::Integer=16_777_216,
)
    values = (
        max_candidate_regimes,
        max_enumeration_work,
        max_cells,
        max_singular_strata,
        max_pair_checks,
        max_facets,
        max_outputs,
        max_matrix_elements,
        max_payload_elements,
        max_finalization_work,
    )
    any(value -> value isa Bool, values) && throw(ArgumentError(
        "RO cell-complex build limits must be integers, not Bool"))
    all(>(0), values) || throw(ArgumentError(
        "all RO cell-complex build limits must be positive"))
    all(value -> value <= typemax(Int), values) || throw(ArgumentError(
        "all RO cell-complex build limits must fit in Int"))
    return ROCellComplexBuildLimits(Int.(values)...)
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

@inline function _ro2_limit(phase::Symbol, requested::BigInt, limit::Int)
    requested <= limit || throw(ROCellComplexLimitExceeded(
        phase, requested, limit))
    return nothing
end

abstract type _RO2SealToken end
struct _RO2ExternalSealToken <: _RO2SealToken end
struct _RO2BuilderSealToken <: _RO2SealToken end
const _RO2_EXTERNAL_SEAL_TOKEN = _RO2ExternalSealToken()
const _RO2_BUILDER_SEAL_TOKEN = _RO2BuilderSealToken()

_ro2_authority_status(::_RO2ExternalSealToken) = :external_unverified
_ro2_authority_status(::_RO2BuilderSealToken) = :engine_replayed

const _RO2_AUTHORITY_STATUSES = (:engine_replayed, :external_unverified)

function _ro2_validate_authority_status(authority_status::Symbol)
    authority_status in _RO2_AUTHORITY_STATUSES || throw(ArgumentError(
        "unsupported RO cell-complex authority status"))
    return nothing
end

function _ro2_preflight_complex_scalars(
    domain::ROInputDomain2D,
    candidate_regime_count::Int,
    regular_candidate_count::Int,
    domain_area::Float64,
    covered_area_sum::Float64,
    gap_area::Union{Nothing,Float64},
    geometry_tolerance::Float64,
    authority_status::Symbol,
    limits::ROCellComplexBuildLimits,
)
    _ro2_validate_authority_status(authority_status)
    candidate_regime_count >= 0 ||
        _ro2_invalid_complex("candidate regime count must be nonnegative")
    regular_candidate_count >= 0 ||
        _ro2_invalid_complex("regular candidate count must be nonnegative")
    regular_candidate_count <= candidate_regime_count ||
        _ro2_invalid_complex("regular candidate count exceeds candidate count")
    _ro2_limit(:candidate_regimes, BigInt(candidate_regime_count),
        limits.max_candidate_regimes)
    isfinite(geometry_tolerance) && geometry_tolerance > 0 ||
        _ro2_invalid_complex("geometry tolerance must be finite and positive")
    spans, derived_domain_area, _, area_tolerance =
        _ro2_certification_tolerances(domain)
    all(isfinite, spans) && isfinite(derived_domain_area) &&
        derived_domain_area > 0 ||
        _ro2_invalid_complex("domain geometry is not finite and positive")
    geometry_tolerance <= _ro2_geometry_tolerance_cap(domain) ||
        _ro2_invalid_complex("geometry tolerance exceeds the domain cap")
    isfinite(domain_area) && domain_area > 0 ||
        _ro2_invalid_complex("domain_area must be finite and positive")
    abs(domain_area - derived_domain_area) <= area_tolerance ||
        _ro2_invalid_complex("domain_area does not match the declared box")
    isfinite(covered_area_sum) && covered_area_sum >= 0 ||
        _ro2_invalid_complex("covered_area_sum must be finite and nonnegative")
    gap_area === nothing || (isfinite(gap_area) && gap_area >= 0) ||
        _ro2_invalid_complex("gap_area must be finite and nonnegative when present")
    return nothing
end

"""
Exact asymptotic two-input cell complex for one binding-equilibrium model and
fixed full q/K background. A successful return represents a complete regime
enumeration under the supplied hard limits; cancellation or a limit throws.
Every array-valued public property is a detached snapshot of sealed backing
storage. A lower-level `getfield` mutation is detected before later public use.
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
    authority_status::Symbol
    content_sha256::String

    function ROCellComplex2D(
        domain::ROInputDomain2D,
        output_indices::Vector{Int},
        cells::Vector{ROCell2D},
        facets::Vector{ROFacet2D},
        singular_strata::Vector{ROSingularStratum2D},
        candidate_regime_count::Int,
        regular_candidate_count::Int,
        domain_area::Float64,
        covered_area_sum::Float64,
        gap_area::Union{Nothing,Float64},
        coverage_complete::Bool,
        has_ambiguity::Bool,
        geometry_tolerance::Float64,
        supplied_content_sha256::Union{Nothing,String},
        limits::ROCellComplexBuildLimits,
        cancel_check,
        seal_token::_RO2SealToken,
    )
        cancel_check()
        authority_status = _ro2_authority_status(seal_token)
        _ro2_preflight_complex_scalars(
            domain,
            candidate_regime_count,
            regular_candidate_count,
            domain_area,
            covered_area_sum,
            gap_area,
            geometry_tolerance,
            authority_status,
            limits,
        )
        _ro2_preflight_complex_components(
            domain,
            output_indices,
            cells,
            facets,
            singular_strata,
            limits,
            cancel_check,
        )
        stored_domain = _ro2_copy_domain(domain, cancel_check)
        stored_output_indices = _ro2_cancellable_copy(
            output_indices, cancel_check)
        stored_cells = [_ro2_copy_cell(cell, cancel_check) for cell in cells]
        stored_facets = [_ro2_copy_facet(facet, cancel_check) for facet in facets]
        stored_strata = [
            _ro2_copy_stratum(stratum, cancel_check)
            for stratum in singular_strata
        ]
        _ro2_validate_complex_components(
            stored_domain,
            stored_output_indices,
            stored_cells,
            stored_facets,
            stored_strata,
            candidate_regime_count,
            regular_candidate_count,
            domain_area,
            covered_area_sum,
            gap_area,
            coverage_complete,
            has_ambiguity,
            geometry_tolerance,
            limits,
            cancel_check,
        )
        expected = _ro2_complex_content_sha256(
            stored_domain,
            stored_output_indices,
            stored_cells,
            stored_facets,
            stored_strata,
            candidate_regime_count,
            regular_candidate_count,
            domain_area,
            covered_area_sum,
            gap_area,
            coverage_complete,
            has_ambiguity,
            geometry_tolerance,
            authority_status;
            cancel_check=cancel_check,
        )
        supplied_content_sha256 === nothing ||
            supplied_content_sha256 == expected || throw(ArgumentError(
            "ROCellComplex2D content seal does not match supplied values"))
        cancel_check()
        return new(
            stored_domain,
            stored_output_indices,
            stored_cells,
            stored_facets,
            stored_strata,
            candidate_regime_count,
            regular_candidate_count,
            domain_area,
            covered_area_sum,
            gap_area,
            coverage_complete,
            has_ambiguity,
            geometry_tolerance,
            authority_status,
            expected,
        )
    end
end

@inline function _ro2_write_int(io::IO, value::Int)
    write(io, htol(reinterpret(UInt64, Int64(value))))
    return nothing
end

@inline function _ro2_write_float(io::IO, value::Float64)
    write(io, htol(reinterpret(UInt64, value)))
    return nothing
end

@inline function _ro2_write_bool(io::IO, value::Bool)
    write(io, UInt8(value))
    return nothing
end

function _ro2_write_string(io::IO, value::AbstractString)
    bytes = codeunits(value)
    write(io, htol(UInt64(length(bytes))))
    write(io, bytes)
    return nothing
end

function _ro2_write_ints(io::IO, values::Vector{Int},
                         cancel_check=_NO_CANCEL_CHECK)
    cancel_check()
    write(io, htol(UInt64(length(values))))
    for value in values
        cancel_check()
        _ro2_write_int(io, value)
    end
    return nothing
end

function _ro2_write_points(io::IO, points::Vector{NTuple{2,Float64}},
                           cancel_check=_NO_CANCEL_CHECK)
    cancel_check()
    write(io, htol(UInt64(length(points))))
    for point in points
        cancel_check()
        _ro2_write_float(io, point[1])
        _ro2_write_float(io, point[2])
    end
    return nothing
end

function _ro2_write_label(io::IO, label::ROAffineLabel2D,
                          cancel_check=_NO_CANCEL_CHECK)
    cancel_check()
    _ro2_write_ints(io, getfield(label, :source_regime_ids), cancel_check)
    matrix = getfield(label, :reaction_order_matrix)
    write(io, htol(UInt64(size(matrix, 1))))
    write(io, htol(UInt64(size(matrix, 2))))
    for value in matrix
        cancel_check()
        _ro2_write_float(io, value)
    end
    offset = getfield(label, :output_offset)
    write(io, htol(UInt64(length(offset))))
    for value in offset
        cancel_check()
        _ro2_write_float(io, value)
    end
    return nothing
end

function _ro2_complex_content_sha256(
    domain::ROInputDomain2D,
    output_indices::Vector{Int},
    cells::Vector{ROCell2D},
    facets::Vector{ROFacet2D},
    singular_strata::Vector{ROSingularStratum2D},
    candidate_regime_count::Int,
    regular_candidate_count::Int,
    domain_area::Float64,
    covered_area_sum::Float64,
    gap_area::Union{Nothing,Float64},
    coverage_complete::Bool,
    has_ambiguity::Bool,
    geometry_tolerance::Float64,
    authority_status::Symbol,
    ;
    cancel_check=_NO_CANCEL_CHECK,
)
    cancel_check()
    io = IOBuffer()
    write(io, codeunits("bne-ro-cell-complex-2d-memory/v2\0"))
    for value in getfield(domain, :axis_indices)
        _ro2_write_int(io, value)
    end
    for value in getfield(domain, :lower_log10)
        _ro2_write_float(io, value)
    end
    for value in getfield(domain, :upper_log10)
        _ro2_write_float(io, value)
    end
    fixed_logqK = getfield(domain, :fixed_logqK)
    write(io, htol(UInt64(length(fixed_logqK))))
    for value in fixed_logqK
        cancel_check()
        _ro2_write_float(io, value)
    end
    _ro2_write_ints(io, output_indices, cancel_check)

    write(io, htol(UInt64(length(cells))))
    for cell in cells
        cancel_check()
        _ro2_write_int(io, getfield(cell, :id))
        _ro2_write_points(io, getfield(cell, :vertices), cancel_check)
        _ro2_write_float(io, getfield(cell, :area))
        _ro2_write_ints(io, getfield(cell, :source_regime_ids), cancel_check)
        labels = getfield(cell, :labels)
        write(io, htol(UInt64(length(labels))))
        for label in labels
            _ro2_write_label(io, label, cancel_check)
        end
        _ro2_write_bool(io, getfield(cell, :set_valued))
    end

    write(io, htol(UInt64(length(facets))))
    for facet in facets
        cancel_check()
        _ro2_write_int(io, getfield(facet, :id))
        _ro2_write_string(io, String(getfield(facet, :kind)))
        for point in getfield(facet, :endpoints), value in point
            _ro2_write_float(io, value)
        end
        _ro2_write_ints(io, getfield(facet, :incident_cell_ids), cancel_check)
        _ro2_write_ints(io, getfield(facet, :singular_stratum_ids), cancel_check)
        for value in getfield(facet, :normal)
            _ro2_write_float(io, value)
        end
        _ro2_write_float(io, getfield(facet, :offset))
        _ro2_write_bool(io, getfield(facet, :mixed_sign))
        side = getfield(facet, :domain_side)
        _ro2_write_bool(io, side !== nothing)
        side === nothing || _ro2_write_string(io, String(side))
    end

    write(io, htol(UInt64(length(singular_strata))))
    for stratum in singular_strata
        cancel_check()
        _ro2_write_int(io, getfield(stratum, :id))
        _ro2_write_int(io, getfield(stratum, :dimension))
        _ro2_write_points(io, getfield(stratum, :vertices), cancel_check)
        _ro2_write_ints(io, getfield(stratum, :source_regime_ids), cancel_check)
        _ro2_write_ints(io, getfield(stratum, :nullities), cancel_check)
        reasons = getfield(stratum, :reasons)
        write(io, htol(UInt64(length(reasons))))
        for reason in reasons
            cancel_check()
            _ro2_write_string(io, String(reason))
        end
    end

    _ro2_write_int(io, candidate_regime_count)
    _ro2_write_int(io, regular_candidate_count)
    _ro2_write_float(io, domain_area)
    _ro2_write_float(io, covered_area_sum)
    _ro2_write_bool(io, gap_area !== nothing)
    gap_area === nothing || _ro2_write_float(io, gap_area)
    _ro2_write_bool(io, coverage_complete)
    _ro2_write_bool(io, has_ambiguity)
    _ro2_write_float(io, geometry_tolerance)
    _ro2_write_string(io, String(authority_status))
    cancel_check()
    return bytes2hex(SHA.sha256(take!(io)))
end

function ROCellComplex2D(
    domain::ROInputDomain2D,
    output_indices::Vector{Int},
    cells::Vector{ROCell2D},
    facets::Vector{ROFacet2D},
    singular_strata::Vector{ROSingularStratum2D},
    candidate_regime_count::Int,
    regular_candidate_count::Int,
    domain_area::Float64,
    covered_area_sum::Float64,
    gap_area::Union{Nothing,Float64},
    coverage_complete::Bool,
    has_ambiguity::Bool,
    geometry_tolerance::Float64,
    ;
    limits::ROCellComplexBuildLimits=ROCellComplexBuildLimits(),
    cancel_check=_NO_CANCEL_CHECK,
)
    return ROCellComplex2D(
        domain,
        output_indices,
        cells,
        facets,
        singular_strata,
        candidate_regime_count,
        regular_candidate_count,
        domain_area,
        covered_area_sum,
        gap_area,
        coverage_complete,
        has_ambiguity,
        geometry_tolerance,
        nothing,
        limits,
        cancel_check,
        _RO2_EXTERNAL_SEAL_TOKEN,
    )
end

function _ro2_assert_unchanged(
    complex::ROCellComplex2D;
    cancel_check=_NO_CANCEL_CHECK,
)
    cancel_check()
    actual = _ro2_complex_content_sha256(
        getfield(complex, :domain),
        getfield(complex, :output_indices),
        getfield(complex, :cells),
        getfield(complex, :facets),
        getfield(complex, :singular_strata),
        getfield(complex, :candidate_regime_count),
        getfield(complex, :regular_candidate_count),
        getfield(complex, :domain_area),
        getfield(complex, :covered_area_sum),
        getfield(complex, :gap_area),
        getfield(complex, :coverage_complete),
        getfield(complex, :has_ambiguity),
        getfield(complex, :geometry_tolerance),
        getfield(complex, :authority_status);
        cancel_check=cancel_check,
    )
    actual == getfield(complex, :content_sha256) || throw(ArgumentError(
        "ROCellComplex2D backing storage changed after construction"))
    cancel_check()
    return nothing
end

function Base.getproperty(complex::ROCellComplex2D, name::Symbol)
    _ro2_assert_unchanged(complex)
    if name === :domain
        return _ro2_copy_domain(getfield(complex, :domain))
    elseif name === :output_indices
        return copy(getfield(complex, :output_indices))
    elseif name === :cells
        return [_ro2_copy_cell(cell) for cell in getfield(complex, :cells)]
    elseif name === :facets
        return [_ro2_copy_facet(facet) for facet in getfield(complex, :facets)]
    elseif name === :singular_strata
        return [
            _ro2_copy_stratum(stratum)
            for stratum in getfield(complex, :singular_strata)
        ]
    end
    return getfield(complex, name)
end

# The geometry tolerance is an implementation aid, not a caller-controlled
# completeness allowance.  Keep it small in both absolute log10 coordinates
# and relative to the narrowest side of the declared box.
const _RO2_MAX_GEOMETRY_TOLERANCE = 1e-6
const _RO2_MAX_RELATIVE_GEOMETRY_TOLERANCE = 1e-6

function _ro2_geometry_tolerance_cap(domain::ROInputDomain2D)
    spans = (
        domain.upper_log10[1] - domain.lower_log10[1],
        domain.upper_log10[2] - domain.lower_log10[2],
    )
    return min(
        _RO2_MAX_GEOMETRY_TOLERANCE,
        _RO2_MAX_RELATIVE_GEOMETRY_TOLERANCE * min(spans...),
    )
end

function _ro2_certification_tolerances(domain::ROInputDomain2D)
    spans = (
        domain.upper_log10[1] - domain.lower_log10[1],
        domain.upper_log10[2] - domain.lower_log10[2],
    )
    minimum_span = min(spans...)
    coordinate_scale = max(
        abs(domain.lower_log10[1]), abs(domain.lower_log10[2]),
        abs(domain.upper_log10[1]), abs(domain.upper_log10[2]), minimum_span,
    )
    length_tolerance = max(
        1e-10 * minimum_span,
        64.0 * eps(Float64) * coordinate_scale,
    )
    domain_area = spans[1] * spans[2]
    area_tolerance = 8.0 * length_tolerance * (spans[1] + spans[2]) +
        128.0 * eps(Float64) * domain_area
    return spans, domain_area, length_tolerance, area_tolerance
end

function _ro2_geometry_validation_work(
    cell_count::BigInt,
    total_vertices::BigInt,
    sum_squared_vertices::BigInt,
    sum_cubed_vertices::BigInt,
    facet_count::BigInt,
    stratum_count::BigInt,
)
    overlap_vertex_work = max(
        BigInt(0),
        (cell_count - 4) * sum_cubed_vertices +
            3 * total_vertices * sum_squared_vertices,
    )
    facet_edge_work = facet_count * total_vertices
    facet_stratum_work = facet_count * stratum_count
    edge_interval_sort_work = facet_edge_work^2
    return total_vertices + sum_squared_vertices + overlap_vertex_work +
        facet_edge_work + facet_stratum_work + edge_interval_sort_work
end

function _ro2_preflight_geometry_work(
    cells,
    facet_count::Integer,
    stratum_count::Integer,
    limits::ROCellComplexBuildLimits,
    cancel_check=_NO_CANCEL_CHECK,
)
    total_vertices = BigInt(0)
    sum_squared_vertices = BigInt(0)
    sum_cubed_vertices = BigInt(0)
    for cell in cells
        cancel_check()
        count = BigInt(length(getfield(cell, :vertices)))
        total_vertices += count
        sum_squared_vertices += count^2
        sum_cubed_vertices += count^3
    end
    requested = _ro2_geometry_validation_work(
        BigInt(length(cells)),
        total_vertices,
        sum_squared_vertices,
        sum_cubed_vertices,
        BigInt(facet_count),
        BigInt(stratum_count),
    )
    _ro2_limit(:finalization_work, requested, limits.max_finalization_work)
    return requested
end

function _ro2_preflight_complex_components(
    domain::ROInputDomain2D,
    output_indices::Vector{Int},
    cells::Vector{ROCell2D},
    facets::Vector{ROFacet2D},
    singular_strata::Vector{ROSingularStratum2D},
    limits::ROCellComplexBuildLimits,
    cancel_check=_NO_CANCEL_CHECK,
)
    cancel_check()
    all(value -> value > 0, (
        limits.max_candidate_regimes,
        limits.max_enumeration_work,
        limits.max_cells,
        limits.max_singular_strata,
        limits.max_pair_checks,
        limits.max_facets,
        limits.max_outputs,
        limits.max_matrix_elements,
        limits.max_payload_elements,
        limits.max_finalization_work,
    )) || throw(ArgumentError("all RO cell-complex build limits must be positive"))

    output_count = BigInt(length(output_indices))
    cell_count = BigInt(length(cells))
    facet_count = BigInt(length(facets))
    stratum_count = BigInt(length(singular_strata))
    pair_checks = cell_count * max(cell_count - 1, BigInt(0)) ÷ 2
    _ro2_limit(:outputs, output_count, limits.max_outputs)
    _ro2_limit(:cells, cell_count, limits.max_cells)
    _ro2_limit(:facets, facet_count, limits.max_facets)
    _ro2_limit(:singular_strata, stratum_count, limits.max_singular_strata)
    _ro2_limit(:pair_checks, pair_checks, limits.max_pair_checks)

    matrix_elements = BigInt(0)
    payload_elements = BigInt(32) +
        BigInt(length(getfield(domain, :fixed_logqK))) + output_count
    _ro2_limit(:payload_elements, payload_elements, limits.max_payload_elements)
    total_vertices = BigInt(0)
    sum_squared_vertices = BigInt(0)
    sum_cubed_vertices = BigInt(0)
    for cell in cells
        cancel_check()
        vertex_count = BigInt(length(getfield(cell, :vertices)))
        total_vertices += vertex_count
        sum_squared_vertices += vertex_count^2
        sum_cubed_vertices += vertex_count^3
        payload_elements += 4 + 2 * vertex_count +
            BigInt(length(getfield(cell, :source_regime_ids)))
        _ro2_limit(:payload_elements, payload_elements,
            limits.max_payload_elements)
        for label in getfield(cell, :labels)
            cancel_check()
            matrix_size = BigInt(length(getfield(label, :reaction_order_matrix)))
            matrix_elements += matrix_size
            _ro2_limit(:matrix_elements, matrix_elements,
                limits.max_matrix_elements)
            payload_elements += 3 +
                BigInt(length(getfield(label, :source_regime_ids))) +
                matrix_size + BigInt(length(getfield(label, :output_offset)))
            _ro2_limit(:payload_elements, payload_elements,
                limits.max_payload_elements)
        end
    end
    for facet in facets
        cancel_check()
        payload_elements += 12 +
            BigInt(length(getfield(facet, :incident_cell_ids))) +
            BigInt(length(getfield(facet, :singular_stratum_ids)))
        _ro2_limit(:payload_elements, payload_elements,
            limits.max_payload_elements)
    end
    for stratum in singular_strata
        cancel_check()
        payload_elements += 3 +
            2 * BigInt(length(getfield(stratum, :vertices))) +
            BigInt(length(getfield(stratum, :source_regime_ids))) +
            BigInt(length(getfield(stratum, :nullities))) +
            BigInt(length(getfield(stratum, :reasons)))
        _ro2_limit(:payload_elements, payload_elements,
            limits.max_payload_elements)
    end
    geometry_work = _ro2_geometry_validation_work(
        cell_count,
        total_vertices,
        sum_squared_vertices,
        sum_cubed_vertices,
        facet_count,
        stratum_count,
    )
    finalization_work = 3 * payload_elements + matrix_elements + pair_checks +
        geometry_work
    _ro2_limit(:finalization_work, finalization_work,
        limits.max_finalization_work)
    cancel_check()
    return nothing
end

"""
Result of classifying one point against an `ROCellComplex2D`. All vector and
label payloads are detached from the sealed source complex.
"""
struct _RO2ClassificationSealToken end
const _RO2_CLASSIFICATION_SEAL_TOKEN = _RO2ClassificationSealToken()

struct ROPointClassification2D
    status::Symbol
    cell_ids::Vector{Int}
    cell_relations::Vector{Symbol}
    facet_ids::Vector{Int}
    singular_stratum_ids::Vector{Int}
    labels::Vector{ROAffineLabel2D}
    source_complex_sha256::String
    source_authority_status::Symbol
    content_sha256::String

    function ROPointClassification2D(
        status::Symbol,
        cell_ids::Vector{Int},
        cell_relations::Vector{Symbol},
        facet_ids::Vector{Int},
        singular_stratum_ids::Vector{Int},
        labels::Vector{ROAffineLabel2D},
        source_complex_sha256::String,
        source_authority_status::Symbol,
        limits::ROCellComplexBuildLimits,
        cancel_check,
        ::_RO2ClassificationSealToken,
    )
        cancel_check()
        occursin(r"^[0-9a-f]{64}$", source_complex_sha256) ||
            throw(ArgumentError(
                "classification requires a canonical source-complex SHA-256"))
        _ro2_validate_authority_status(source_authority_status)
        _ro2_preflight_classification(
            cell_ids,
            cell_relations,
            facet_ids,
            singular_stratum_ids,
            labels,
            limits,
            cancel_check,
        )
        stored_cell_ids = _ro2_cancellable_copy(cell_ids, cancel_check)
        stored_relations = _ro2_cancellable_copy(
            cell_relations, cancel_check)
        stored_facet_ids = _ro2_cancellable_copy(facet_ids, cancel_check)
        stored_stratum_ids = _ro2_cancellable_copy(
            singular_stratum_ids, cancel_check)
        stored_labels = [
            _ro2_copy_label(label, cancel_check) for label in labels]
        _ro2_validate_classification(
            status,
            stored_cell_ids,
            stored_relations,
            stored_facet_ids,
            stored_stratum_ids,
            stored_labels,
            cancel_check,
        )
        content_sha256 = _ro2_classification_content_sha256(
            status,
            stored_cell_ids,
            stored_relations,
            stored_facet_ids,
            stored_stratum_ids,
            stored_labels,
            source_complex_sha256,
            source_authority_status,
            cancel_check=cancel_check,
        )
        cancel_check()
        return new(
            status,
            stored_cell_ids,
            stored_relations,
            stored_facet_ids,
            stored_stratum_ids,
            stored_labels,
            source_complex_sha256,
            source_authority_status,
            content_sha256,
        )
    end
end

function _ro2_validate_classification(
    status::Symbol,
    cell_ids::Vector{Int},
    cell_relations::Vector{Symbol},
    facet_ids::Vector{Int},
    singular_stratum_ids::Vector{Int},
    labels::Vector{ROAffineLabel2D},
    cancel_check=_NO_CANCEL_CHECK,
)
    cancel_check()
    status in (:cell, :boundary, :ambiguity, :gap, :outside_domain) ||
        throw(ArgumentError("unsupported RO point-classification status"))
    all(>(0), cell_ids) && issorted(cell_ids) && allunique(cell_ids) ||
        throw(ArgumentError("classification cell ids must be positive, sorted, and unique"))
    length(cell_relations) == length(cell_ids) ||
        throw(ArgumentError("classification cell ids and relations disagree"))
    all(relation -> relation in (:interior, :boundary), cell_relations) ||
        throw(ArgumentError("classification contains an unsupported cell relation"))
    all(>(0), facet_ids) && issorted(facet_ids) && allunique(facet_ids) ||
        throw(ArgumentError("classification facet ids must be positive, sorted, and unique"))
    all(>(0), singular_stratum_ids) && issorted(singular_stratum_ids) &&
        allunique(singular_stratum_ids) || throw(ArgumentError(
        "classification singular-stratum ids must be positive, sorted, and unique"))

    label_keys = String[]
    output_count = nothing
    for label in labels
        cancel_check()
        sources = getfield(label, :source_regime_ids)
        !isempty(sources) && all(>(0), sources) && issorted(sources) &&
            allunique(sources) || throw(ArgumentError(
            "classification label source ids must be positive, sorted, and unique"))
        matrix = getfield(label, :reaction_order_matrix)
        offset = getfield(label, :output_offset)
        size(matrix, 2) == 2 || throw(DimensionMismatch(
            "classification label matrices must have two input columns"))
        size(matrix, 1) > 0 && length(offset) == size(matrix, 1) ||
            throw(DimensionMismatch(
                "classification label matrix and offset shapes disagree"))
        all(isfinite, matrix) && all(isfinite, offset) ||
            throw(ArgumentError("classification affine labels must be finite"))
        output_count === nothing && (output_count = size(matrix, 1))
        size(matrix, 1) == output_count || throw(DimensionMismatch(
            "classification labels must share one output count"))
        push!(label_keys, _ro2_label_key(label, cancel_check))
    end
    allunique(label_keys) ||
        throw(ArgumentError(
            "classification affine labels must have unique scientific identities"))

    if status in (:gap, :outside_domain)
        isempty(cell_ids) && isempty(cell_relations) && isempty(facet_ids) &&
            isempty(singular_stratum_ids) && isempty(labels) ||
            throw(ArgumentError("gap/outside classifications cannot contain evidence"))
    elseif status === :cell
        length(cell_ids) == 1 && cell_relations == [:interior] &&
            isempty(facet_ids) && isempty(singular_stratum_ids) &&
            length(labels) == 1 || throw(ArgumentError(
            "cell classification requires one interior cell and one affine label"))
    elseif status === :boundary
        relations_are_boundary = all(==(:boundary), cell_relations)
        interior_singular_boundary = length(cell_ids) == 1 &&
            cell_relations == [:interior] && !isempty(singular_stratum_ids)
        relations_are_boundary || interior_singular_boundary ||
            throw(ArgumentError(
                "boundary classification has inconsistent cell relations"))
        (!isempty(cell_ids) || !isempty(facet_ids) ||
            !isempty(singular_stratum_ids)) || throw(ArgumentError(
            "boundary classification requires boundary evidence"))
        isempty(cell_ids) == isempty(labels) || throw(ArgumentError(
            "boundary cell evidence and affine labels must be present together"))
    else
        !isempty(cell_ids) && !isempty(labels) &&
            (length(cell_ids) > 1 || length(labels) > 1) ||
            throw(ArgumentError(
                "ambiguity classification requires multiple cells or labels"))
    end
    return nothing
end

function _ro2_preflight_classification(
    cell_ids::Vector{Int},
    cell_relations::Vector{Symbol},
    facet_ids::Vector{Int},
    singular_stratum_ids::Vector{Int},
    labels::Vector{ROAffineLabel2D},
    limits::ROCellComplexBuildLimits,
    cancel_check=_NO_CANCEL_CHECK,
)
    cancel_check()
    _ro2_limit(:cells, BigInt(length(cell_ids)), limits.max_cells)
    _ro2_limit(:facets, BigInt(length(facet_ids)), limits.max_facets)
    _ro2_limit(:singular_strata, BigInt(length(singular_stratum_ids)),
        limits.max_singular_strata)
    matrix_elements = BigInt(0)
    payload_elements = BigInt(8 + length(cell_ids) + length(cell_relations) +
        length(facet_ids) + length(singular_stratum_ids))
    _ro2_limit(:payload_elements, payload_elements, limits.max_payload_elements)
    for label in labels
        cancel_check()
        matrix = getfield(label, :reaction_order_matrix)
        output_count = BigInt(size(matrix, 1))
        _ro2_limit(:outputs, output_count, limits.max_outputs)
        matrix_elements += BigInt(length(matrix))
        _ro2_limit(:matrix_elements, matrix_elements,
            limits.max_matrix_elements)
        payload_elements += 3 +
            BigInt(length(getfield(label, :source_regime_ids))) +
            BigInt(length(matrix)) +
            BigInt(length(getfield(label, :output_offset)))
        _ro2_limit(:payload_elements, payload_elements,
            limits.max_payload_elements)
    end
    _ro2_limit(:finalization_work,
        3 * payload_elements + matrix_elements,
        limits.max_finalization_work)
    cancel_check()
    return nothing
end

function _ro2_classification_content_sha256(
    status::Symbol,
    cell_ids::Vector{Int},
    cell_relations::Vector{Symbol},
    facet_ids::Vector{Int},
    singular_stratum_ids::Vector{Int},
    labels::Vector{ROAffineLabel2D},
    source_complex_sha256::String,
    source_authority_status::Symbol,
    ;
    cancel_check=_NO_CANCEL_CHECK,
)
    cancel_check()
    io = IOBuffer()
    write(io, codeunits("bne-ro-point-classification-2d-memory/v2\0"))
    _ro2_write_string(io, source_complex_sha256)
    _ro2_write_string(io, String(source_authority_status))
    _ro2_write_string(io, String(status))
    _ro2_write_ints(io, cell_ids, cancel_check)
    write(io, htol(UInt64(length(cell_relations))))
    for relation in cell_relations
        cancel_check()
        _ro2_write_string(io, String(relation))
    end
    _ro2_write_ints(io, facet_ids, cancel_check)
    _ro2_write_ints(io, singular_stratum_ids, cancel_check)
    write(io, htol(UInt64(length(labels))))
    for label in labels
        cancel_check()
        _ro2_write_label(io, label, cancel_check)
    end
    cancel_check()
    return bytes2hex(SHA.sha256(take!(io)))
end

function ROPointClassification2D(
    status::Symbol,
    cell_ids::Vector{Int},
    cell_relations::Vector{Symbol},
    facet_ids::Vector{Int},
    singular_stratum_ids::Vector{Int},
    labels::Vector{ROAffineLabel2D},
    ;
    limits::ROCellComplexBuildLimits=ROCellComplexBuildLimits(),
    cancel_check=_NO_CANCEL_CHECK,
)
    throw(ArgumentError(
        "RO point classifications are producer-only; classify a sealed complex"))
end

function _ro2_assert_classification_unchanged(
    result::ROPointClassification2D;
    cancel_check=_NO_CANCEL_CHECK,
)
    cancel_check()
    actual = _ro2_classification_content_sha256(
        getfield(result, :status),
        getfield(result, :cell_ids),
        getfield(result, :cell_relations),
        getfield(result, :facet_ids),
        getfield(result, :singular_stratum_ids),
        getfield(result, :labels),
        getfield(result, :source_complex_sha256),
        getfield(result, :source_authority_status),
        cancel_check=cancel_check,
    )
    actual == getfield(result, :content_sha256) || throw(ArgumentError(
        "ROPointClassification2D backing storage changed after construction"))
    return nothing
end

function validate_ro_point_classification(
    complex::ROCellComplex2D,
    point,
    result::ROPointClassification2D;
    tolerance::Union{Nothing,Real}=nothing,
    limits::ROCellComplexBuildLimits=ROCellComplexBuildLimits(),
    cancel_check=_NO_CANCEL_CHECK,
)
    cancel_check()
    _ro2_assert_classification_unchanged(
        result; cancel_check=cancel_check)
    _ro2_assert_unchanged(complex; cancel_check=cancel_check)
    getfield(result, :source_complex_sha256) ==
        getfield(complex, :content_sha256) || throw(ArgumentError(
        "classification source complex does not match replay input"))
    getfield(result, :source_authority_status) ===
        getfield(complex, :authority_status) || throw(ArgumentError(
        "classification source authority does not match replay input"))
    replayed = classify_ro_cell_complex_point(
        complex,
        point;
        tolerance=tolerance,
        limits=limits,
        cancel_check=cancel_check,
    )
    getfield(replayed, :content_sha256) == getfield(result, :content_sha256) ||
        throw(ArgumentError(
            "classification does not match source-bound replay"))
    cancel_check()
    return result
end

function Base.getproperty(result::ROPointClassification2D, name::Symbol)
    _ro2_assert_classification_unchanged(result)
    if name === :cell_ids
        return copy(getfield(result, :cell_ids))
    elseif name === :cell_relations
        return copy(getfield(result, :cell_relations))
    elseif name === :facet_ids
        return copy(getfield(result, :facet_ids))
    elseif name === :singular_stratum_ids
        return copy(getfield(result, :singular_stratum_ids))
    elseif name === :labels
        return [_ro2_copy_label(label) for label in getfield(result, :labels)]
    end
    return getfield(result, name)
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
@inline _ro2_distance(a, b) = hypot(a[1] - b[1], a[2] - b[2])

function _ro2_canonical_point(p, tolerance)
    # Canonicalization must not manufacture an equivalence relation.  Only
    # signed zero is normalized here; tolerance equivalence is decided by the
    # explicit metric comparison below.
    x = iszero(p[1]) ? 0.0 : Float64(p[1])
    y = iszero(p[2]) ? 0.0 : Float64(p[2])
    return (x, y)
end

function _ro2_unique_points(
    points,
    tolerance,
    cancel_check=_NO_CANCEL_CHECK,
)
    ordered = NTuple{2,Float64}[]
    sizehint!(ordered, length(points))
    for point in points
        cancel_check()
        push!(ordered, _ro2_canonical_point(point, tolerance))
    end
    cancel_check()
    sort!(ordered)
    cancel_check()
    result = NTuple{2,Float64}[]
    for point in ordered
        duplicate = false
        for existing in result
            cancel_check()
            if _ro2_distance(existing, point) <= tolerance
                duplicate = true
                break
            end
        end
        duplicate || push!(result, point)
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

function _ro2_canonical_vertices(
    points,
    dimension::Int,
    tolerance,
    cancel_check=_NO_CANCEL_CHECK,
)
    unique_points = _ro2_unique_points(points, tolerance, cancel_check)
    if dimension <= 0
        return isempty(unique_points) ? unique_points : unique_points[1:1]
    elseif dimension == 1
        length(unique_points) <= 2 && return unique_points
        best_pair = (1, 2)
        best_distance = -Inf
        for i in 1:(length(unique_points) - 1), j in (i + 1):length(unique_points)
            cancel_check()
            distance = _ro2_distance(unique_points[i], unique_points[j])
            if distance > best_distance
                best_distance = distance
                best_pair = (i, j)
            end
        end
        return sort!([unique_points[best_pair[1]], unique_points[best_pair[2]]])
    end

    length(unique_points) >= 3 || return unique_points
    x_total = 0.0
    y_total = 0.0
    for point in unique_points
        cancel_check()
        x_total += point[1]
        y_total += point[2]
    end
    centroid = (x_total / length(unique_points),
        y_total / length(unique_points))
    cancel_check()
    sort!(unique_points; by=p -> (
        atan(p[2] - centroid[2], p[1] - centroid[1]),
        _ro2_distance(p, centroid),
    ))
    cancel_check()
    _ro2_signed_area(unique_points) < 0 && reverse!(unique_points)
    return _ro2_rotate_to_lexicographic_start!(unique_points)
end

function _ro2_key(vertices, dimension::Int, tolerance)
    # This is only a candidate-bucket key.  Do not truncate at 14 decimals:
    # sub-1e-14 domains can legitimately distinguish adjacent Float64 cells.
    digits = max(ceil(Int, -log10(tolerance)) + 2, 0)
    coordinate(value) = begin
        rounded = round(Float64(value); digits=digits)
        iszero(rounded) ? "0" : string(rounded)
    end
    return string(dimension, ":", join(
        (string(coordinate(p[1]), ",", coordinate(p[2])) for p in vertices),
        ";",
    ))
end

function _ro2_vertices_equivalent(
    left,
    right,
    tolerance::Float64,
    cancel_check=_NO_CANCEL_CHECK,
)
    length(left) == length(right) || return false
    for index in eachindex(left, right)
        cancel_check()
        _ro2_distance(left[index], right[index]) <= tolerance ||
            return false
    end
    return true
end

function _ro2_segments_equivalent(
    left,
    right,
    tolerance::Float64,
    cancel_check=_NO_CANCEL_CHECK,
)
    cancel_check()
    direct = _ro2_distance(left[1], right[1]) <= tolerance &&
        _ro2_distance(left[2], right[2]) <= tolerance
    direct && return true
    cancel_check()
    return _ro2_distance(left[1], right[2]) <= tolerance &&
        _ro2_distance(left[2], right[1]) <= tolerance
end

function _ro2_exact_geometry_key(
    vertices,
    dimension::Int,
    cancel_check=_NO_CANCEL_CHECK,
)
    io = IOBuffer()
    write(io, htol(UInt64(dimension)))
    write(io, htol(UInt64(length(vertices))))
    for point in vertices, value in point
        cancel_check()
        normalized = iszero(value) ? 0.0 : value
        write(io, htol(reinterpret(UInt64, normalized)))
    end
    return bytes2hex(SHA.sha256(take!(io)))
end

function _ro2_resolve_candidate_key(
    candidates,
    _bucket_key::String,
    vertices,
    dimension::Int,
    tolerance::Float64,
    comparison_work::Base.RefValue{BigInt},
    limits::ROCellComplexBuildLimits,
    cancel_check=_NO_CANCEL_CHECK,
)
    # Decimal buckets are an acceleration hint only: equivalent geometries can
    # straddle a rounding boundary, while distinct tiny-domain geometries can
    # share a decimal representation.  Compare every dimension/vertex-count
    # compatible candidate under the declared metric, with that work charged.
    # Dict iteration order is intentionally unspecified.  Sort exact storage
    # keys so both identity resolution and its metered work are reproducible
    # across processes and supported Julia versions.
    matched_key = nothing
    for candidate_key in sort!(collect(keys(candidates)))
        cancel_check()
        candidate = candidates[candidate_key]
        # Charge the compatibility scan itself before touching candidate
        # payload; incompatible dimensions/counts can otherwise form an
        # unmetered O(candidate_count^2) path.
        comparison_work[] += BigInt(1)
        _ro2_limit(
            :pair_checks,
            comparison_work[],
            limits.max_pair_checks,
        )
        hasfield(typeof(candidate), :dimension) &&
            getfield(candidate, :dimension) != dimension && continue
        candidate_vertices = getfield(candidate, :vertices)
        length(candidate_vertices) == length(vertices) || continue
        # A compatible metric comparison costs one unit per vertex in total;
        # the baseline scan above already paid for its first vertex.
        additional_metric_work = max(length(vertices) - 1, 0)
        comparison_work[] += BigInt(additional_metric_work)
        _ro2_limit(
            :pair_checks,
            comparison_work[],
            limits.max_pair_checks,
        )
        equivalent = _ro2_vertices_equivalent(
            candidate_vertices,
            vertices,
            tolerance,
            cancel_check,
        )
        if equivalent
            matched_key === nothing || error(
                "internal duplicate equivalent candidate geometries")
            matched_key = candidate_key
        end
    end
    matched_key === nothing || return matched_key
    exact_key = string(
        dimension,
        ":",
        length(vertices),
        "#",
        _ro2_exact_geometry_key(vertices, dimension, cancel_check),
    )
    haskey(candidates, exact_key) &&
        error("internal exact geometry-key collision")
    return exact_key
end

function _ro2_label_key(
    label::ROAffineLabel2D,
    cancel_check=_NO_CANCEL_CHECK,
)
    # Geometry tolerance may merge numerically equivalent vertices, but it is
    # not an equivalence relation for scientific outputs. Preserve the exact
    # Float64 identity of every affine coefficient, normalizing only signed
    # zero, which has identical arithmetic semantics.
    matrix = label.reaction_order_matrix
    offset = label.output_offset
    io = IOBuffer()
    write(io, htol(UInt64(size(matrix, 1))))
    write(io, htol(UInt64(size(matrix, 2))))
    for value in matrix
        cancel_check()
        normalized = iszero(value) ? 0.0 : value
        write(io, htol(reinterpret(UInt64, normalized)))
    end
    write(io, htol(UInt64(length(offset))))
    for value in offset
        cancel_check()
        normalized = iszero(value) ? 0.0 : value
        write(io, htol(reinterpret(UInt64, normalized)))
    end
    cancel_check()
    return bytes2hex(SHA.sha256(take!(io)))
end

function _ro2_unique_labels(labels, cancel_check=_NO_CANCEL_CHECK)
    by_key = Dict{String,ROAffineLabel2D}()
    for label in labels
        cancel_check()
        key = _ro2_label_key(label, cancel_check)
        if haskey(by_key, key)
            previous = by_key[key]
            combined_sources = Int[]
            sizehint!(combined_sources,
                length(previous.source_regime_ids) +
                    length(label.source_regime_ids))
            for sources in (
                previous.source_regime_ids, label.source_regime_ids)
                for source in sources
                    cancel_check()
                    push!(combined_sources, source)
                end
            end
            by_key[key] = ROAffineLabel2D(
                sort!(unique(combined_sources)),
                _ro2_cancellable_copy(
                    previous.reaction_order_matrix, cancel_check),
                _ro2_cancellable_copy(previous.output_offset, cancel_check),
            )
        else
            by_key[key] = _ro2_copy_label(label, cancel_check)
        end
    end
    cancel_check()
    return [by_key[key] for key in sort!(collect(keys(by_key)))]
end

function _ro2_polyhedron_vertices(
    poly,
    tolerance,
    limits::ROCellComplexBuildLimits,
    matrix_footprint::Base.RefValue{BigInt},
    construction_work::Base.RefValue{BigInt},
    cancel_check=_NO_CANCEL_CHECK,
)
    cancel_check()
    hrepresentation = MixedMatHRep(hrep(poly))
    constraint_count = BigInt(size(hrepresentation.A, 1))
    h_footprint = BigInt(length(hrepresentation.A)) +
        BigInt(length(hrepresentation.b))
    matrix_footprint[] = max(matrix_footprint[], h_footprint)
    _ro2_limit(:matrix_elements, matrix_footprint[],
        limits.max_matrix_elements)
    # A bounded planar H-polyhedron has at most one vertex per supporting
    # inequality.  Reserve a cubic 2D half-space kernel bound plus the complete
    # downstream vertex conversion/deduplication cost before invoking CDD.
    kernel_reservation = constraint_count^3 +
        3 * constraint_count^2 + 3 * constraint_count
    construction_work[] += kernel_reservation
    _ro2_limit(:finalization_work, construction_work[],
        limits.max_finalization_work)
    cancel_check()
    representation = MixedMatVRep(vrep(poly))
    cancel_check()
    size(representation.R, 1) == 0 || error(
        "internal error: rectangular clipping produced an unbounded 2D polyhedron")
    vertex_count = BigInt(size(representation.V, 1))
    coordinate_count = BigInt(size(representation.V, 2))
    coordinate_count == 2 || error(
        "internal error: expected a two-dimensional sliced polyhedron")
    matrix_footprint[] = max(
        matrix_footprint[], vertex_count * coordinate_count)
    _ro2_limit(:matrix_elements, matrix_footprint[],
        limits.max_matrix_elements)
    actual_payload_work = vertex_count * coordinate_count +
        vertex_count + vertex_count^2
    reserved_payload_work = 3 * constraint_count^2 + 3 * constraint_count
    if actual_payload_work > reserved_payload_work
        construction_work[] += actual_payload_work - reserved_payload_work
        _ro2_limit(:finalization_work, construction_work[],
            limits.max_finalization_work)
    end
    points = NTuple{2,Float64}[]
    sizehint!(points, Int(vertex_count))
    for index in axes(representation.V, 1)
        cancel_check()
        push!(points, (
            Float64(representation.V[index, 1]),
            Float64(representation.V[index, 2]),
        ))
    end
    return _ro2_canonical_vertices(
        points, dim(poly), tolerance, cancel_check)
end

function _ro2_regime_polyhedron(
    regime,
    domain::ROInputDomain2D,
    limits::ROCellComplexBuildLimits,
    matrix_footprint::Base.RefValue{BigInt},
    construction_work::Base.RefValue{BigInt},
    cancel_check=_NO_CANCEL_CHECK,
)
    cancel_check()
    raw_C, raw_C0, nullity = get_C_C0_nullity_qK(regime)
    row_count, column_count = size(raw_C)
    length(raw_C0) == row_count || error(
        "internal regime condition matrix/offset shape mismatch")
    raw_footprint = BigInt(length(raw_C)) + BigInt(length(raw_C0))
    matrix_footprint[] = max(matrix_footprint[], raw_footprint)
    _ro2_limit(:matrix_elements, matrix_footprint[],
        limits.max_matrix_elements)
    axes = getfield(domain, :axis_indices)
    base = _ro2_cancellable_collect(
        getfield(domain, :fixed_logqK), Float64, cancel_check)
    column_count == length(base) || error(
        "internal regime condition matrix/domain shape mismatch")
    base[axes[1]] = 0.0
    base[axes[2]] = 0.0

    combined_C = Matrix{Float64}(undef, row_count + 4, 2)
    combined_C0 = Vector{Float64}(undef, row_count + 4)
    construction_work[] += BigInt(row_count) *
        (BigInt(column_count) + 3)
    _ro2_limit(:finalization_work, construction_work[],
        limits.max_finalization_work)
    for row in 1:row_count
        cancel_check()
        combined_C[row, 1] = Float64(raw_C[row, axes[1]])
        combined_C[row, 2] = Float64(raw_C[row, axes[2]])
        offset = Float64(raw_C0[row])
        for column in 1:column_count
            cancel_check()
            offset += Float64(raw_C[row, column]) * base[column]
        end
        combined_C0[row] = offset
    end
    lower = domain.lower_log10
    upper = domain.upper_log10
    combined_C[row_count + 1, :] .= (1.0, 0.0)
    combined_C[row_count + 2, :] .= (-1.0, 0.0)
    combined_C[row_count + 3, :] .= (0.0, 1.0)
    combined_C[row_count + 4, :] .= (0.0, -1.0)
    combined_C0[(row_count + 1):end] .=
        (-lower[1], upper[1], -lower[2], upper[2])
    cancel_check()
    poly = get_polyhedron(combined_C, combined_C0, nullity)
    detecthlinearity!(poly)
    cancel_check()
    return poly, nullity, base
end

function _ro2_affine_label(
    regime,
    output_indices,
    axes,
    base,
    limits::ROCellComplexBuildLimits,
    matrix_footprint::Base.RefValue{BigInt},
    construction_work::Base.RefValue{BigInt},
    cancel_check=_NO_CANCEL_CHECK,
)
    cancel_check()
    H, H0 = get_H_H0(regime)
    H === nothing && error("regular regime $(regime.idx) has no affine gain matrix")
    H0 === nothing && error("regular regime $(regime.idx) has no affine offset")
    size(H, 2) == length(base) || error(
        "internal affine gain/domain shape mismatch")
    length(H0) == size(H, 1) || error(
        "internal affine gain/offset shape mismatch")
    raw_footprint = BigInt(length(H)) + BigInt(length(H0))
    matrix_footprint[] = max(matrix_footprint[], raw_footprint)
    _ro2_limit(:matrix_elements, matrix_footprint[],
        limits.max_matrix_elements)
    output_count = length(output_indices)
    matrix = Matrix{Float64}(undef, output_count, 2)
    offset = Vector{Float64}(undef, output_count)
    construction_work[] += BigInt(output_count) *
        (BigInt(length(base)) + 3)
    _ro2_limit(:finalization_work, construction_work[],
        limits.max_finalization_work)
    for (row, output_index) in enumerate(output_indices)
        cancel_check()
        matrix[row, 1] = Float64(H[output_index, axes[1]])
        matrix[row, 2] = Float64(H[output_index, axes[2]])
        value = Float64(H0[output_index])
        for column in eachindex(base)
            cancel_check()
            value += Float64(H[output_index, column]) * base[column]
        end
        offset[row] = value
    end
    return ROAffineLabel2D(
        [regime.idx],
        matrix,
        offset,
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
    scale = _ro2_distance(p, p2)
    scale > tolerance || return nothing
    unit = (direction[1] / scale, direction[2] / scale)
    perpendicular_distance(point) = abs(
        unit[1] * (point[2] - p[2]) - unit[2] * (point[1] - p[1]))
    perpendicular_distance(q) <= tolerance ||
        return nothing
    perpendicular_distance(q2) <= tolerance ||
        return nothing

    q_distance = _ro2_dot(_ro2_sub(q, p), unit)
    q2_distance = _ro2_dot(_ro2_sub(q2, p), unit)
    length_tolerance = max(tolerance, 128.0 * eps(Float64) * scale)
    low = max(0.0, min(q_distance, q2_distance))
    high = min(scale, max(q_distance, q2_distance))
    low <= scale + length_tolerance && high >= -length_tolerance ||
        return nothing
    high - low > tolerance || return nothing
    point_at(distance) = if abs(distance) <= length_tolerance
        p
    elseif abs(distance - scale) <= length_tolerance
        p2
    else
        _ro2_add(p, _ro2_scale(unit, distance))
    end
    endpoints = sort!([
        _ro2_canonical_point(point_at(low), tolerance),
        _ro2_canonical_point(point_at(high), tolerance),
    ])
    return (endpoints[1], endpoints[2])
end

function _ro2_edges(vertices, cancel_check=_NO_CANCEL_CHECK)
    edges = NTuple{2,NTuple{2,Float64}}[]
    for i in eachindex(vertices)
        cancel_check()
        j = i == length(vertices) ? 1 : i + 1
        push!(edges, (vertices[i], vertices[j]))
    end
    return edges
end

function _ro2_intersection_line(s, e, s_distance, e_distance)
    segment_direction = _ro2_sub(e, s)
    denominator = s_distance - e_distance
    iszero(denominator) && return e
    t = clamp(s_distance / denominator, 0.0, 1.0)
    return _ro2_add(s, _ro2_scale(segment_direction, t))
end

function _ro2_convex_intersection(
    subject,
    clip,
    tolerance,
    cancel_check=_NO_CANCEL_CHECK,
)
    output = _ro2_cancellable_copy(subject, cancel_check)
    for (a, b) in _ro2_edges(clip, cancel_check)
        cancel_check()
        clip_direction = _ro2_sub(b, a)
        clip_length = hypot(clip_direction...)
        clip_length > tolerance || return NTuple{2,Float64}[]
        clip_unit = (
            clip_direction[1] / clip_length,
            clip_direction[2] / clip_length,
        )
        signed_distance(point) =
            clip_unit[1] * (point[2] - a[2]) -
                clip_unit[2] * (point[1] - a[1])
        input = output
        output = NTuple{2,Float64}[]
        isempty(input) && break
        s = input[end]
        s_distance = signed_distance(s)
        s_inside = s_distance >= -tolerance
        for e in input
            cancel_check()
            e_distance = signed_distance(e)
            e_inside = e_distance >= -tolerance
            if e_inside
                if !s_inside
                    push!(output,
                        _ro2_intersection_line(s, e, s_distance, e_distance))
                end
                push!(output, e)
            elseif s_inside
                push!(output,
                    _ro2_intersection_line(s, e, s_distance, e_distance))
            end
            s = e
            s_distance = e_distance
            s_inside = e_inside
        end
        output = _ro2_canonical_vertices(
            output, 2, tolerance, cancel_check)
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
    magnitude = hypot(normal...)
    magnitude > tolerance || error("internal error: zero-length facet")
    normal = (normal[1] / magnitude, normal[2] / magnitude)
    if normal[1] < 0 || (iszero(normal[1]) && normal[2] < 0)
        normal = (-normal[1], -normal[2])
    end
    offset = -_ro2_dot(normal, endpoints[1])
    mixed_sign = normal[1] * normal[2] < 0
    return normal, offset, mixed_sign
end

function _ro2_point_on_segment(point, endpoints, tolerance)
    a, b = endpoints
    direction = _ro2_sub(b, a)
    scale = _ro2_distance(a, b)
    scale > tolerance || return _ro2_distance(point, a) <= tolerance
    unit = (direction[1] / scale, direction[2] / scale)
    abs(unit[1] * (point[2] - a[2]) - unit[2] * (point[1] - a[1])) <= tolerance ||
        return false
    projection = _ro2_dot(_ro2_sub(point, a), unit)
    length_tolerance = max(tolerance, 128.0 * eps(Float64) * scale)
    return -length_tolerance <= projection <= scale + length_tolerance
end

function _ro2_point_cell_relation(
    vertices,
    point,
    tolerance,
    cancel_check=_NO_CANCEL_CHECK,
)
    boundary = false
    for (a, b) in _ro2_edges(vertices, cancel_check)
        cancel_check()
        direction = _ro2_sub(b, a)
        scale = hypot(direction...)
        scale > tolerance || return :outside
        unit = (direction[1] / scale, direction[2] / scale)
        side = unit[1] * (point[2] - a[2]) -
            unit[2] * (point[1] - a[1])
        side < -tolerance && return :outside
        abs(side) <= tolerance && (boundary = true)
    end
    return boundary ? :boundary : :interior
end

function _ro2_stratum_contains(stratum, point, tolerance)
    isempty(stratum.vertices) && return false
    stratum.dimension == 0 &&
        return _ro2_distance(stratum.vertices[1], point) <= tolerance
    stratum.dimension == 1 && length(stratum.vertices) >= 2 &&
        return _ro2_point_on_segment(point, (stratum.vertices[1], stratum.vertices[end]), tolerance)
    return false
end

@noinline function _ro2_invalid_complex(message::AbstractString)
    throw(ArgumentError("invalid ROCellComplex2D: " * message))
end

function _ro2_segment_interval_on_edge(segment, edge, tolerance::Float64)
    a, b = edge
    direction = _ro2_sub(b, a)
    edge_length = _ro2_distance(a, b)
    edge_length > tolerance || return nothing
    unit = (direction[1] / edge_length, direction[2] / edge_length)
    parameter_tolerance = max(128.0 * eps(Float64), tolerance / edge_length)
    parameters = Float64[]
    for point in segment
        _ro2_point_on_segment(point, edge, tolerance) || return nothing
        parameter = _ro2_dot(_ro2_sub(point, a), unit) / edge_length
        -parameter_tolerance <= parameter <= 1.0 + parameter_tolerance ||
            return nothing
        push!(parameters, clamp(parameter, 0.0, 1.0))
    end
    lower, upper = minmax(parameters...)
    (upper - lower) * edge_length > tolerance || return nothing
    return lower, upper, parameter_tolerance
end

function _ro2_validate_complex_components(
    domain::ROInputDomain2D,
    output_indices::Vector{Int},
    cells::Vector{ROCell2D},
    facets::Vector{ROFacet2D},
    singular_strata::Vector{ROSingularStratum2D},
    candidate_regime_count::Int,
    regular_candidate_count::Int,
    domain_area::Float64,
    covered_area_sum::Float64,
    gap_area::Union{Nothing,Float64},
    coverage_complete::Bool,
    has_ambiguity::Bool,
    geometry_tolerance::Float64,
    limits::ROCellComplexBuildLimits,
    cancel_check=_NO_CANCEL_CHECK,
)
    cancel_check()
    fixed_logqK = getfield(domain, :fixed_logqK)
    axes = getfield(domain, :axis_indices)
    all(index -> 1 <= index <= length(fixed_logqK), axes) ||
        _ro2_invalid_complex("domain axes must index fixed_logqK")
    all(isfinite, fixed_logqK) ||
        _ro2_invalid_complex("fixed_logqK must be finite")
    isempty(output_indices) &&
        _ro2_invalid_complex("at least one output index is required")
    all(index -> 1 <= index <= length(fixed_logqK), output_indices) ||
        _ro2_invalid_complex("output indices must index fixed_logqK")
    allunique(output_indices) ||
        _ro2_invalid_complex("output indices must be unique")

    candidate_regime_count >= 0 ||
        _ro2_invalid_complex("candidate regime count must be nonnegative")
    regular_candidate_count >= 0 ||
        _ro2_invalid_complex("regular candidate count must be nonnegative")
    regular_candidate_count <= candidate_regime_count ||
        _ro2_invalid_complex("regular candidate count exceeds candidate count")
    _ro2_limit(:candidate_regimes, BigInt(candidate_regime_count),
        limits.max_candidate_regimes)

    isfinite(geometry_tolerance) && geometry_tolerance > 0 ||
        _ro2_invalid_complex("geometry tolerance must be finite and positive")
    geometry_tolerance <= _ro2_geometry_tolerance_cap(domain) ||
        _ro2_invalid_complex("geometry tolerance exceeds the domain cap")
    spans, derived_domain_area, length_tolerance, area_tolerance =
        _ro2_certification_tolerances(domain)
    all(isfinite, spans) && isfinite(derived_domain_area) &&
        derived_domain_area > 0 ||
        _ro2_invalid_complex("domain geometry is not finite and positive")
    isfinite(domain_area) && domain_area > 0 ||
        _ro2_invalid_complex("domain_area must be finite and positive")
    abs(domain_area - derived_domain_area) <= area_tolerance ||
        _ro2_invalid_complex("domain_area does not match the declared box")
    isfinite(covered_area_sum) && covered_area_sum >= 0 ||
        _ro2_invalid_complex("covered_area_sum must be finite and nonnegative")

    lower = getfield(domain, :lower_log10)
    upper = getfield(domain, :upper_log10)
    output_count = length(output_indices)
    regular_sources = Set{Int}()
    computed_area_sum = 0.0
    edge_intervals = Dict{Tuple{Int,Int},
        Vector{Tuple{Float64,Float64,Float64,Int}}}()
    for (position, cell) in enumerate(cells)
        cancel_check()
        getfield(cell, :id) == position ||
            _ro2_invalid_complex("cell ids must be canonical and consecutive")
        vertices = getfield(cell, :vertices)
        length(vertices) >= 3 ||
            _ro2_invalid_complex("each cell must have at least three vertices")
        all(point -> all(isfinite, point), vertices) ||
            _ro2_invalid_complex("cell vertices must be finite")
        all(point ->
                lower[1] - length_tolerance <= point[1] <=
                    upper[1] + length_tolerance &&
                lower[2] - length_tolerance <= point[2] <=
                    upper[2] + length_tolerance,
            vertices) || _ro2_invalid_complex("cell lies outside the domain")
        length(_ro2_unique_points(
            vertices, length_tolerance, cancel_check)) ==
            length(vertices) ||
            _ro2_invalid_complex("cell vertices must be distinct")
        first(vertices) == minimum(vertices) ||
            _ro2_invalid_complex("cell vertices must have a canonical start")
        signed_area = _ro2_signed_area(vertices)
        isfinite(signed_area) && signed_area > area_tolerance ||
            _ro2_invalid_complex("cell must be finite, full-dimensional, and CCW")
        for vertex_index in eachindex(vertices)
            previous_index = vertex_index == 1 ? length(vertices) : vertex_index - 1
            following_index = vertex_index == length(vertices) ? 1 : vertex_index + 1
            incoming = _ro2_sub(vertices[vertex_index], vertices[previous_index])
            outgoing = _ro2_sub(vertices[following_index], vertices[vertex_index])
            _ro2_cross(incoming, outgoing) >= -area_tolerance ||
                _ro2_invalid_complex("cell must be convex")
            edge_intervals[(position, vertex_index)] =
                Tuple{Float64,Float64,Float64,Int}[]
        end
        area = _ro2_area(vertices)
        isfinite(getfield(cell, :area)) &&
            abs(getfield(cell, :area) - area) <= area_tolerance ||
            _ro2_invalid_complex("stored cell area does not match its vertices")
        computed_area_sum += area

        cell_sources = getfield(cell, :source_regime_ids)
        !isempty(cell_sources) && all(>(0), cell_sources) &&
            issorted(cell_sources) && allunique(cell_sources) ||
            _ro2_invalid_complex("cell source ids must be positive, sorted, and unique")
        all(source -> !(source in regular_sources), cell_sources) ||
            _ro2_invalid_complex("a regular source id occurs in multiple cells")
        union!(regular_sources, cell_sources)

        labels = getfield(cell, :labels)
        isempty(labels) && _ro2_invalid_complex("each cell requires an affine label")
        label_keys = String[]
        labelled_sources = Int[]
        for label in labels
            cancel_check()
            label_sources = getfield(label, :source_regime_ids)
            !isempty(label_sources) && all(>(0), label_sources) &&
                issorted(label_sources) && allunique(label_sources) ||
                _ro2_invalid_complex(
                    "label source ids must be positive, sorted, and unique")
            append!(labelled_sources, label_sources)
            matrix = getfield(label, :reaction_order_matrix)
            offset = getfield(label, :output_offset)
            size(matrix) == (output_count, 2) ||
                _ro2_invalid_complex("affine label matrix has the wrong shape")
            length(offset) == output_count ||
                _ro2_invalid_complex("affine label offset has the wrong shape")
            all(isfinite, matrix) && all(isfinite, offset) ||
                _ro2_invalid_complex("affine labels must be finite")
            push!(label_keys, _ro2_label_key(label, cancel_check))
        end
        allunique(label_keys) ||
            _ro2_invalid_complex("affine labels must have unique scientific identities")
        sort(labelled_sources) == cell_sources && allunique(labelled_sources) ||
            _ro2_invalid_complex("label sources must partition the cell sources")
        getfield(cell, :set_valued) == (length(labels) > 1) ||
            _ro2_invalid_complex("set_valued disagrees with affine-label multiplicity")
    end
    regular_candidate_count == length(regular_sources) ||
        _ro2_invalid_complex("regular candidate count disagrees with cell sources")

    singular_sources = Set{Int}()
    allowed_reasons = Set((:singular_regime, :lower_dimensional_slice))
    for (position, stratum) in enumerate(singular_strata)
        cancel_check()
        getfield(stratum, :id) == position ||
            _ro2_invalid_complex("singular-stratum ids must be canonical and consecutive")
        dimension = getfield(stratum, :dimension)
        dimension in (0, 1) ||
            _ro2_invalid_complex("singular strata must have dimension zero or one")
        vertices = getfield(stratum, :vertices)
        length(vertices) == dimension + 1 ||
            _ro2_invalid_complex("singular-stratum vertices disagree with dimension")
        all(point -> all(isfinite, point), vertices) ||
            _ro2_invalid_complex("singular-stratum vertices must be finite")
        issorted(vertices) ||
            _ro2_invalid_complex("singular-stratum vertices must be canonical")
        all(point ->
                lower[1] - length_tolerance <= point[1] <=
                    upper[1] + length_tolerance &&
                lower[2] - length_tolerance <= point[2] <=
                    upper[2] + length_tolerance,
            vertices) ||
            _ro2_invalid_complex("singular stratum lies outside the domain")
        dimension == 0 ||
            _ro2_distance(vertices[1], vertices[2]) > length_tolerance ||
            _ro2_invalid_complex("one-dimensional stratum must have positive length")

        sources = getfield(stratum, :source_regime_ids)
        !isempty(sources) && all(>(0), sources) && issorted(sources) &&
            allunique(sources) || _ro2_invalid_complex(
            "singular-stratum source ids must be positive, sorted, and unique")
        all(source -> !(source in regular_sources) && !(source in singular_sources),
            sources) || _ro2_invalid_complex(
            "source ids must identify one serialized cell or stratum")
        union!(singular_sources, sources)
        nullities = getfield(stratum, :nullities)
        !isempty(nullities) && all(>=(0), nullities) && issorted(nullities) &&
            allunique(nullities) || _ro2_invalid_complex(
            "singular-stratum nullities must be nonnegative, sorted, and unique")
        reasons = getfield(stratum, :reasons)
        !isempty(reasons) && all(reason -> reason in allowed_reasons, reasons) &&
            reasons == sort(reasons; by=string) && allunique(reasons) ||
            _ro2_invalid_complex("singular-stratum reasons are not canonical")
        (:singular_regime in reasons) == any(>(0), nullities) ||
            _ro2_invalid_complex("singular-regime reason disagrees with nullity")
        (:lower_dimensional_slice in reasons) == any(==(0), nullities) ||
            _ro2_invalid_complex("lower-dimensional reason disagrees with nullity")
    end
    length(regular_sources) + length(singular_sources) <= candidate_regime_count ||
        _ro2_invalid_complex("serialized sources exceed the candidate population")

    facet_keys = Set{Any}()
    for (position, facet) in enumerate(facets)
        cancel_check()
        getfield(facet, :id) == position ||
            _ro2_invalid_complex("facet ids must be canonical and consecutive")
        kind = getfield(facet, :kind)
        kind in (:internal, :domain) ||
            _ro2_invalid_complex("facet kind is unsupported")
        endpoints = getfield(facet, :endpoints)
        all(point -> all(isfinite, point), endpoints) ||
            _ro2_invalid_complex("facet endpoints must be finite")
        endpoints[1] < endpoints[2] ||
            _ro2_invalid_complex("facet endpoints must be canonical and distinct")
        _ro2_distance(endpoints[1], endpoints[2]) > length_tolerance ||
            _ro2_invalid_complex("facet must have positive length")
        key = (kind, endpoints, getfield(facet, :domain_side))
        key in facet_keys && _ro2_invalid_complex("duplicate facet geometry")
        push!(facet_keys, key)

        incidence = getfield(facet, :incident_cell_ids)
        expected_count = kind === :internal ? 2 : 1
        length(incidence) == expected_count && all(>(0), incidence) &&
            issorted(incidence) && allunique(incidence) &&
            all(<=(length(cells)), incidence) ||
            _ro2_invalid_complex("facet incidence is not canonical")
        actual_incidence = Int[]
        memberships = Tuple{Int,Int,NTuple{3,Float64}}[]
        for cell in cells
            matching = Tuple{Int,NTuple{3,Float64}}[]
            for (edge_index, edge) in enumerate(_ro2_edges(
                getfield(cell, :vertices), cancel_check))
                cancel_check()
                interval = _ro2_segment_interval_on_edge(
                    endpoints, edge, length_tolerance)
                interval === nothing || push!(matching, (edge_index, interval))
            end
            length(matching) <= 1 ||
                _ro2_invalid_complex("facet matches multiple edges of one cell")
            if length(matching) == 1
                edge_index, interval = only(matching)
                push!(actual_incidence, getfield(cell, :id))
                push!(memberships, (getfield(cell, :id), edge_index, interval))
            end
        end
        sort!(actual_incidence)
        actual_incidence == incidence ||
            _ro2_invalid_complex("facet incidence disagrees with cell geometry")

        side = _ro2_domain_side(endpoints, domain, length_tolerance)
        if kind === :domain
            side !== nothing && side == getfield(facet, :domain_side) ||
                _ro2_invalid_complex("domain facet side disagrees with geometry")
        else
            side === nothing && getfield(facet, :domain_side) === nothing ||
                _ro2_invalid_complex("internal facet cannot be a domain facet")
        end

        normal = getfield(facet, :normal)
        all(isfinite, normal) && isfinite(getfield(facet, :offset)) ||
            _ro2_invalid_complex("facet normal and offset must be finite")
        expected_normal, expected_offset, expected_mixed =
            _ro2_facet_geometry(endpoints, length_tolerance)
        isapprox(normal[1], expected_normal[1]; rtol=0.0, atol=512eps(Float64)) &&
            isapprox(normal[2], expected_normal[2]; rtol=0.0, atol=512eps(Float64)) ||
            _ro2_invalid_complex("facet normal is not canonical")
        abs(getfield(facet, :offset) - expected_offset) <= length_tolerance ||
            _ro2_invalid_complex("facet offset disagrees with its endpoints")
        getfield(facet, :mixed_sign) == expected_mixed ||
            _ro2_invalid_complex("facet mixed_sign flag is inconsistent")

        singular_ids = getfield(facet, :singular_stratum_ids)
        all(>(0), singular_ids) && issorted(singular_ids) &&
            allunique(singular_ids) &&
            all(<=(length(singular_strata)), singular_ids) ||
            _ro2_invalid_complex("facet singular-stratum incidence is not canonical")
        actual_singular_ids = Int[]
        for stratum in singular_strata
            getfield(stratum, :dimension) == 1 || continue
            overlap = _ro2_segment_overlap(
                endpoints,
                (getfield(stratum, :vertices)[1], getfield(stratum, :vertices)[2]),
                length_tolerance,
            )
            overlap === nothing || push!(actual_singular_ids, getfield(stratum, :id))
        end
        actual_singular_ids == singular_ids ||
            _ro2_invalid_complex(
                "facet singular-stratum incidence disagrees with geometry")
        for (cell_id, edge_index, interval) in memberships
            low, high, parameter_tolerance = interval
            push!(edge_intervals[(cell_id, edge_index)],
                (low, high, parameter_tolerance, position))
        end
    end

    for intervals in values(edge_intervals)
        cancel_check()
        isempty(intervals) && continue
        sort!(intervals; by=item -> (item[1], item[2], item[4]))
        cursor = 0.0
        for (low, high, parameter_tolerance, _) in intervals
            low < cursor - parameter_tolerance &&
                _ro2_invalid_complex("cell-edge facets overlap")
            coverage_complete && low > cursor + parameter_tolerance &&
                _ro2_invalid_complex("complete complex has an uncovered cell edge")
            cursor = max(cursor, high)
        end
        if coverage_complete
            final_tolerance = maximum(item[3] for item in intervals)
            cursor >= 1.0 - final_tolerance ||
                _ro2_invalid_complex("complete complex has an uncovered cell edge")
        end
    end
    if coverage_complete
        all(intervals -> !isempty(intervals), values(edge_intervals)) ||
            _ro2_invalid_complex("complete complex is missing a cell-boundary facet")
    end

    isfinite(computed_area_sum) &&
        abs(covered_area_sum - computed_area_sum) <= area_tolerance ||
        _ro2_invalid_complex("covered_area_sum does not match cell geometry")
    positive_area_overlap = false
    if length(cells) >= 2
        for left_index in 1:(length(cells) - 1),
            right_index in (left_index + 1):length(cells)
            cancel_check()
            overlap = _ro2_convex_intersection(
                getfield(cells[left_index], :vertices),
                getfield(cells[right_index], :vertices),
                length_tolerance,
                cancel_check,
            )
            _ro2_area(overlap) > area_tolerance &&
                (positive_area_overlap = true)
        end
    end
    certification_resolvable =
        length_tolerance <= 1e-5 * min(spans...)
    expected_coverage = certification_resolvable && !positive_area_overlap &&
        abs(computed_area_sum - derived_domain_area) <= area_tolerance
    coverage_complete == expected_coverage ||
        _ro2_invalid_complex("coverage_complete disagrees with reconstructed geometry")
    expected_ambiguity = positive_area_overlap ||
        any(cell -> getfield(cell, :set_valued), cells)
    has_ambiguity == expected_ambiguity ||
        _ro2_invalid_complex("has_ambiguity disagrees with reconstructed geometry")
    if positive_area_overlap
        gap_area === nothing ||
            _ro2_invalid_complex("overlapping cells require an unknown gap area")
    else
        gap_area !== nothing && isfinite(gap_area) && gap_area >= 0 ||
            _ro2_invalid_complex("nonoverlapping cells require a finite nonnegative gap")
        expected_gap = max(0.0, derived_domain_area - computed_area_sum)
        abs(gap_area - expected_gap) <= area_tolerance ||
            _ro2_invalid_complex("gap_area disagrees with reconstructed geometry")
    end
    cancel_check()
    return nothing
end

function _ro2_add_facet!(accumulators, kind, endpoints, cell_ids, domain_side,
    tolerance, limits, comparison_work, cancel_check=_NO_CANCEL_CHECK)
    canonical = sort!([endpoints[1], endpoints[2]])
    segment = (canonical[1], canonical[2])
    for accumulator in values(accumulators)
        cancel_check()
        comparison_work[] += BigInt(1)
        _ro2_limit(
            :finalization_work,
            comparison_work[],
            limits.max_finalization_work,
        )
        getfield(accumulator, :kind) === kind || continue
        getfield(accumulator, :domain_side) === domain_side || continue
        comparison_work[] += BigInt(2)
        _ro2_limit(
            :finalization_work,
            comparison_work[],
            limits.max_finalization_work,
        )
        if _ro2_segments_equivalent(
            getfield(accumulator, :endpoints),
            segment,
            tolerance,
            cancel_check,
        )
            union!(getfield(accumulator, :incident_cell_ids), cell_ids)
            return
        end
    end
    requested = BigInt(length(accumulators)) + 1
    requested <= limits.max_facets || throw(ROCellComplexLimitExceeded(
        :facets, requested, limits.max_facets))
    key = string(
        kind === :internal ? "I:" : "D:",
        domain_side === nothing ? "" : string(domain_side),
        ":",
        _ro2_exact_geometry_key(collect(segment), 1, cancel_check),
    )
    haskey(accumulators, key) && error("internal exact facet-key collision")
    accumulators[key] = _ROFacetAccumulator2D(
        kind, segment, Set(Int.(cell_ids)), domain_side)
end

function _ro2_preflight_model_matrix_elements(
    model::Bnc,
    output_count::Integer,
    limits::ROCellComplexBuildLimits,
)
    helper = model._L_helper
    assignment_upper_bound = prod(
        (BigInt(length(choices)) for choices in helper.J);
        init=BigInt(1),
    )
    state_count = BigInt(model.n)
    row_count = BigInt(length(helper.J))
    condition_count = BigInt(helper.total_constraints)
    # Singular affine projection can expose more qK facets than either the
    # source condition count or the state dimension.  Without relying on an
    # unproved network-specific projection theorem, bound its rows by the
    # number of source-facet subsets (every projected face has at least one
    # such active-set witness).
    qk_condition_count = max(
        BigInt(1) << Int(condition_count), state_count)
    # One completed regime may retain its permutation/P/C data plus dense
    # affine and qK-condition caches.  Count the full logical shapes rather
    # than sparse nonzeros so the admission remains representation independent.
    per_regime_cache =
        # perm + P + P0
        row_count + row_count * state_count + row_count +
        # M + M0
        state_count^2 + state_count +
        # C_x + C0_x
        condition_count * state_count + condition_count +
        # H + H0
        state_count^2 + state_count +
        # Singular qK projection uses the active-set facet bound above; it can
        # expose substantially more rows than the source condition population.
        qk_condition_count * state_count + qk_condition_count
    cache_reservation = assignment_upper_bound * per_regime_cache
    asymptotic_upper_bound = min(
        assignment_upper_bound, BigInt(limits.max_candidate_regimes))
    published_label_reservation = asymptotic_upper_bound *
        BigInt(output_count) * 3
    condition_transient =
        qk_condition_count * state_count + qk_condition_count +
        3 * (qk_condition_count + 4) + state_count
    affine_transient = state_count^2 + state_count +
        3 * BigInt(output_count)
    requested = cache_reservation + published_label_reservation +
        max(condition_transient, affine_transient)
    _ro2_limit(:matrix_elements, requested, limits.max_matrix_elements)
    return requested
end

function _ro2_preflight_model_projection_work(
    model::Bnc,
    limits::ROCellComplexBuildLimits,
)
    helper = model._L_helper
    assignment_upper_bound = prod(
        (BigInt(length(choices)) for choices in helper.J);
        init=BigInt(1),
    )
    state_count = BigInt(model.n)
    condition_count = BigInt(helper.total_constraints)
    projected_condition_bound = max(
        BigInt(1) << Int(condition_count), state_count)

    # `find_all_regimes!` materializes affine caches before the 2D builder can
    # inspect an individual regime.  A singular cache fill may enter the CDD
    # projection/h-representation kernel and expose one projected facet for
    # each source active-set witness.  That kernel is not cooperatively
    # cancellable, so reserve its complete conservative materialization work
    # before starting regime enumeration or touching the model cache.  Count
    # every possible assignment because non-asymptotic leaves are cached too.
    per_assignment_work = projected_condition_bound *
        max(condition_count, BigInt(1)) * max(state_count, BigInt(1))
    requested = assignment_upper_bound * per_assignment_work
    _ro2_limit(
        :finalization_work,
        requested,
        limits.max_finalization_work,
    )
    return requested
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
    spans, domain_area, certification_length_tolerance, area_tolerance =
        _ro2_certification_tolerances(domain)
    all(isfinite, spans) && isfinite(domain_area) && domain_area > 0 &&
        isfinite(certification_length_tolerance) &&
        certification_length_tolerance > 0 && isfinite(area_tolerance) &&
        area_tolerance > 0 || throw(ArgumentError(
        "RO cell-complex domain geometry is not finite and positive"))
    maximum_tolerance = _ro2_geometry_tolerance_cap(domain)
    tolerance <= maximum_tolerance || throw(ArgumentError(
        "geometry_tolerance must not exceed $(maximum_tolerance) for this " *
        "domain (absolute cap=$(_RO2_MAX_GEOMETRY_TOLERANCE), relative " *
        "cap=$(_RO2_MAX_RELATIVE_GEOMETRY_TOLERANCE) of the shortest side)"))
    isnothing(model.catalysis) || throw(ArgumentError(
        "exact 2D RO cell complexes currently support binding equilibrium only"))
    axes = domain.axis_indices
    all(i -> 1 <= i <= model.d, axes) || throw(ArgumentError(
        "RO cell-complex axes must be two conserved-total q coordinates in 1:$(model.d)"))
    length(getfield(domain, :fixed_logqK)) == model.n || throw(DimensionMismatch(
        "fixed_logqK must contain all $(model.n) q/K coordinates"))
    _ro2_limit(:outputs, BigInt(length(output_indices)), limits.max_outputs)
    _ro2_preflight_model_matrix_elements(
        model, length(output_indices), limits)
    projection_work_reservation = _ro2_preflight_model_projection_work(
        model, limits)
    cancel_check()
    outputs = _ro2_cancellable_collect(output_indices, Int, cancel_check)
    isempty(outputs) && throw(ArgumentError("at least one species output is required"))
    length(unique(outputs)) == length(outputs) || throw(ArgumentError(
        "output_indices must be unique and ordered"))
    all(i -> 1 <= i <= model.n, outputs) || throw(BoundsError(1:model.n, outputs))

    construction_work = Ref(projection_work_reservation)
    enumeration_work = Ref(BigInt(0))
    matrix_footprint = Ref(BigInt(0))
    cancel_check()
    try
        find_all_regimes!(
            model;
            cancel_check=cancel_check,
            max_asymptotic_regimes=limits.max_candidate_regimes,
            max_enumeration_work=limits.max_enumeration_work,
            enumeration_work_counter=enumeration_work,
        )
    catch err
        if err isa _AsymptoticRegimeLimitExceeded
            throw(ROCellComplexLimitExceeded(
                :candidate_regimes, err.requested, err.limit))
        elseif err isa _RegimeEnumerationWorkLimitExceeded
            throw(ROCellComplexLimitExceeded(
                :enumeration_work, err.requested, err.limit))
        end
        rethrow()
    end
    cancel_check()
    source_regimes = _bind_regimes_data(model)
    candidate_count = count(is_asymptotic, source_regimes)
    BigInt(candidate_count) <= limits.max_candidate_regimes ||
        throw(ROCellComplexLimitExceeded(
            :candidate_regimes, BigInt(candidate_count), limits.max_candidate_regimes))
    label_matrix_reservation = BigInt(candidate_count) *
        BigInt(length(outputs)) * 2
    _ro2_limit(:matrix_elements, label_matrix_reservation,
        limits.max_matrix_elements)
    label_payload_reservation = BigInt(candidate_count) *
        (BigInt(length(outputs)) * 3 + 4)
    _ro2_limit(:payload_elements, label_payload_reservation,
        limits.max_payload_elements)
    cancel_check()

    cell_candidates = Dict{String,_ROCellCandidate2D}()
    stratum_candidates = Dict{String,_ROStratumCandidate2D}()
    candidate_geometry_comparison_work = Ref(BigInt(0))
    regular_candidate_count = 0
    for regime in source_regimes
        is_asymptotic(regime) || continue
        cancel_check()
        poly, nullity, base = _ro2_regime_polyhedron(
            regime,
            domain,
            limits,
            matrix_footprint,
            construction_work,
            cancel_check,
        )
        isempty(poly) && continue
        dimension = dim(poly)
        dimension < 0 && continue
        vertices = _ro2_polyhedron_vertices(
            poly,
            tolerance,
            limits,
            matrix_footprint,
            construction_work,
            cancel_check,
        )
        isempty(vertices) && continue
        bucket_key = _ro2_key(vertices, dimension, tolerance)

        if nullity == 0 && dimension == 2 &&
            sqrt(_ro2_area(vertices)) > tolerance
            regular_candidate_count += 1
            label = _ro2_affine_label(
                regime,
                outputs,
                axes,
                base,
                limits,
                matrix_footprint,
                construction_work,
                cancel_check,
            )
            key = _ro2_resolve_candidate_key(
                cell_candidates,
                bucket_key,
                vertices,
                dimension,
                tolerance,
                candidate_geometry_comparison_work,
                limits,
                cancel_check,
            )
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
            key = _ro2_resolve_candidate_key(
                stratum_candidates,
                bucket_key,
                vertices,
                dimension,
                tolerance,
                candidate_geometry_comparison_work,
                limits,
                cancel_check,
            )
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
        cancel_check()
        candidate = cell_candidates[key]
        labels = _ro2_unique_labels(candidate.labels, cancel_check)
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
        cancel_check()
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
    pair_checks = candidate_geometry_comparison_work[] +
        BigInt(length(cells)) * BigInt(max(length(cells) - 1, 0)) ÷ 2
    pair_checks <= limits.max_pair_checks || throw(ROCellComplexLimitExceeded(
        :pair_checks, pair_checks, limits.max_pair_checks))
    base_geometry_work = _ro2_preflight_geometry_work(
        cells, 0, length(singular_strata), limits, cancel_check)
    candidate_geometry_comparison_work[] = pair_checks
    facet_comparison_work = Ref(construction_work[] + base_geometry_work)
    cancel_check()

    facet_accumulators = Dict{String,_ROFacetAccumulator2D}()
    positive_area_overlap = false
    if length(cells) >= 2
        for i in 1:(length(cells) - 1), j in (i + 1):length(cells)
            cancel_check()
            cell_i, cell_j = cells[i], cells[j]
            intersection = _ro2_convex_intersection(
                cell_i.vertices, cell_j.vertices, tolerance, cancel_check)
            _ro2_area(intersection) > area_tolerance &&
                (positive_area_overlap = true)

            for edge_i in _ro2_edges(cell_i.vertices, cancel_check),
                edge_j in _ro2_edges(cell_j.vertices, cancel_check)
                cancel_check()
                overlap = _ro2_segment_overlap(edge_i, edge_j, tolerance)
                overlap === nothing || _ro2_add_facet!(
                    facet_accumulators,
                    :internal,
                    overlap,
                    (cell_i.id, cell_j.id),
                    nothing,
                    tolerance,
                    limits,
                    facet_comparison_work,
                    cancel_check,
                )
            end
        end
    end
    for cell in cells
        cancel_check()
        for edge in _ro2_edges(cell.vertices, cancel_check)
            cancel_check()
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
                facet_comparison_work,
                cancel_check,
            )
        end
    end
    cancel_check()

    final_geometry_work = _ro2_preflight_geometry_work(
        cells,
        length(facet_accumulators),
        length(singular_strata),
        limits,
        cancel_check,
    )
    facet_scan_work = facet_comparison_work[] -
        construction_work[] - base_geometry_work
    _ro2_limit(
        :finalization_work,
        construction_work[] + final_geometry_work + facet_scan_work,
        limits.max_finalization_work,
    )
    facets = ROFacet2D[]
    for key in sort!(collect(keys(facet_accumulators)))
        cancel_check()
        accumulator = facet_accumulators[key]
        normal, offset, mixed_sign = _ro2_facet_geometry(
            accumulator.endpoints, tolerance)
        singular_ids = Int[]
        for stratum in singular_strata
            cancel_check()
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

    covered_area = sum((cell.area for cell in cells); init=0.0)
    # Coverage certification uses its own domain/Float64-derived tolerance.
    # The caller's geometry tolerance may guide clipping and deduplication but
    # cannot relax the final area claim.
    certification_resolvable =
        certification_length_tolerance <= 1e-5 * min(spans...)
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
        nothing,
        limits,
        cancel_check,
        _RO2_BUILDER_SEAL_TOKEN,
    )
end

"""
    classify_ro_cell_complex_point(complex, point;
        tolerance=nothing, limits=ROCellComplexBuildLimits(),
        cancel_check=()->nothing)

Classify a log10 input point without guessing through gaps or degeneracies.
Statuses are `:cell`, `:boundary`, `:ambiguity`, `:gap`, or
`:outside_domain`. Boundary and ambiguity results retain all incident cells,
facets, singular strata, and unique affine labels. The query tolerance is an
implementation aid and is capped by the same absolute and domain-relative
bounds as cell-complex construction.
"""
function classify_ro_cell_complex_point(
    complex::ROCellComplex2D,
    point;
    tolerance::Union{Nothing,Real}=nothing,
    limits::ROCellComplexBuildLimits=ROCellComplexBuildLimits(),
    cancel_check=_NO_CANCEL_CHECK,
)
    cancel_check()
    domain = getfield(complex, :domain)
    cells = getfield(complex, :cells)
    facets = getfield(complex, :facets)
    singular_strata = getfield(complex, :singular_strata)
    _ro2_preflight_complex_components(
        domain,
        getfield(complex, :output_indices),
        cells,
        facets,
        singular_strata,
        limits,
        cancel_check,
    )
    _ro2_assert_unchanged(complex; cancel_check=cancel_check)
    length(point) == 2 || throw(DimensionMismatch(
        "RO cell-complex point must contain two log10 coordinates"))
    all(value -> value isa Real && !(value isa Bool), point) || throw(
        ArgumentError("classification point must contain real values"))
    p = (Float64(point[1]), Float64(point[2]))
    all(isfinite, p) || throw(ArgumentError("classification point must be finite"))
    tol = tolerance === nothing ?
        getfield(complex, :geometry_tolerance) : Float64(tolerance)
    isfinite(tol) && tol > 0 || throw(ArgumentError(
        "classification tolerance must be finite and positive"))
    maximum_tolerance = _ro2_geometry_tolerance_cap(domain)
    tol <= maximum_tolerance || throw(ArgumentError(
        "classification tolerance must not exceed $(maximum_tolerance) for " *
        "this domain (absolute cap=$(_RO2_MAX_GEOMETRY_TOLERANCE), relative " *
        "cap=$(_RO2_MAX_RELATIVE_GEOMETRY_TOLERANCE) of the shortest side)"))
    lower, upper = domain.lower_log10, domain.upper_log10
    if p[1] < lower[1] - tol || p[1] > upper[1] + tol ||
        p[2] < lower[2] - tol || p[2] > upper[2] + tol
        return ROPointClassification2D(
            :outside_domain,
            Int[],
            Symbol[],
            Int[],
            Int[],
            ROAffineLabel2D[],
            getfield(complex, :content_sha256),
            getfield(complex, :authority_status),
            limits,
            cancel_check,
            _RO2_CLASSIFICATION_SEAL_TOKEN,
        )
    end

    cell_ids = Int[]
    relations = Symbol[]
    collected_labels = ROAffineLabel2D[]
    set_valued = false
    for cell in cells
        cancel_check()
        relation = _ro2_point_cell_relation(
            cell.vertices, p, tol, cancel_check)
        relation === :outside && continue
        push!(cell_ids, cell.id)
        push!(relations, relation)
        for label in cell.labels
            cancel_check()
            push!(collected_labels, label)
        end
        set_valued |= cell.set_valued
    end
    facet_ids = Int[]
    for facet in facets
        cancel_check()
        _ro2_point_on_segment(p, facet.endpoints, tol) &&
            push!(facet_ids, facet.id)
    end
    singular_ids = Int[]
    for stratum in singular_strata
        cancel_check()
        _ro2_stratum_contains(stratum, p, tol) &&
            push!(singular_ids, stratum.id)
    end
    labels = _ro2_unique_labels(collected_labels, cancel_check)

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
        getfield(complex, :content_sha256),
        getfield(complex, :authority_status),
        limits,
        cancel_check,
        _RO2_CLASSIFICATION_SEAL_TOKEN,
    )
end
