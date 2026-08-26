# Versioned identity for explicit convex-affine D=3 Float64 enumerated
# consistency. The persisted 2D RPB2 contract is unchanged.
import JSON3
import SHA

const RO_CELL_COMPLEX_3D_VERSION = "bne-ro-cell-complex-3d/v2.1.0"
const RO_CELL_COMPLEX_3D_EVIDENCE_SCOPE =
    :explicit_convex_affine_d3_float64_dyadic_exact_enumerated_consistency
const _RO3_MAX_PHYSICAL_CONSTRUCTION_TOLERANCE = 1.0e-6
const _RO3_MAX_RELATIVE_CONSTRUCTION_TOLERANCE = 1.0e-6
const _RO3_MAX_KEY_BYTES = 1_024
const _RO3_MAX_REASON_BYTES = 1_024
const _RO3_MAX_FIXED_BACKGROUND = 10_000_000
const _RO3_MAX_HALFSPACES_PER_CELL = 16_384
const _RO3_MAX_LABELS = 10_000_000
const _RO3_MAX_SOURCE_REGIME_IDS = 100_000_000
const _RO3_MAX_OUTPUTS = 1_000_000
const _RO3_MAX_INPUT_SCALAR_WORK = 2_000_000_000

struct _RO3ValidatedToken end
const _RO3_VALIDATED = _RO3ValidatedToken()

function _ro3_bounded_length(raw, name::AbstractString, hard_max::Int)
    (raw isa AbstractVector || raw isa Tuple) || throw(ArgumentError(
        "$(name) must be an indexable finite collection"))
    amount = length(raw)
    amount <= hard_max || throw(ArgumentError(
        "$(name) length $(amount) exceeds the absolute hard maximum $(hard_max)"))
    return amount
end

"""A declared three-axis box embedded in one fixed full log-q/K background."""
struct ROInputDomain3D
    axis_indices::NTuple{3,Int}
    lower_log10::NTuple{3,Float64}
    upper_log10::NTuple{3,Float64}
    fixed_logqK::Vector{Float64}

    function ROInputDomain3D(::_RO3ValidatedToken, args...)
        new(args...)
    end
end

function ROInputDomain3D(axis_indices, lower_log10, upper_log10, fixed_logqK)
    _ro3_bounded_length(axis_indices, "3D RO domain axes", 3) == 3 ||
        throw(ArgumentError(
        "a 3D RO domain requires exactly three ordered axes"))
    all(value -> value isa Integer && !(value isa Bool), axis_indices) ||
        throw(ArgumentError("3D RO domain axes must be integers"))
    axes = Tuple(Int.(axis_indices))
    length(unique(axes)) == 3 || throw(ArgumentError(
        "3D RO domain axes must be distinct"))
    _ro3_bounded_length(fixed_logqK, "fixed log-q/K background",
        _RO3_MAX_FIXED_BACKGROUND)
    fixed = Float64.(fixed_logqK)
    all(index -> 1 <= index <= length(fixed), axes) || throw(ArgumentError(
        "3D RO domain axes must index the fixed log-q/K background"))
    _ro3_bounded_length(lower_log10, "3D RO domain lower bound", 3) == 3 ||
        throw(DimensionMismatch(
        "3D RO domain lower bound must have length three"))
    _ro3_bounded_length(upper_log10, "3D RO domain upper bound", 3) == 3 ||
        throw(DimensionMismatch(
        "3D RO domain upper bound must have length three"))
    lower = Tuple(Float64.(lower_log10))
    upper = Tuple(Float64.(upper_log10))
    all(isfinite, lower) && all(isfinite, upper) && all(isfinite, fixed) ||
        throw(ArgumentError("3D RO domain coordinates must be finite"))
    all(upper[i] > lower[i] for i in 1:3) || throw(ArgumentError(
        "each 3D RO domain upper bound must exceed its lower bound"))
    return ROInputDomain3D(_RO3_VALIDATED, axes, lower, upper, copy(fixed))
end

"""One full MIMO affine label retained intact on a regular 3D cell."""
struct ROAffineLabel3D
    source_regime_ids::Vector{Int}
    reaction_order_matrix::Matrix{Float64}
    output_offset::Vector{Float64}

    function ROAffineLabel3D(::_RO3ValidatedToken, args...)
        new(args...)
    end
end


function _ro3_source_ids(raw, name::AbstractString;
    cancel_check=() -> nothing)
    _ro3_bounded_length(raw, name, _RO3_MAX_SOURCE_REGIME_IDS)
    sources = Int[]
    seen = Set{Int}()
    sizehint!(sources, length(raw))
    for raw_value in raw
        _ro3_checkpoint(cancel_check)
        raw_value isa Integer && !(raw_value isa Bool) ||
            throw(ArgumentError("$(name) must contain integers"))
        value = Int(raw_value)
        value > 0 || throw(ArgumentError(
            "$(name) must contain positive identifiers"))
        if !(value in seen)
            push!(seen, value)
            push!(sources, value)
        end
    end
    _ro3_cancellable_sort!(sources, cancel_check)
    isempty(sources) && throw(ArgumentError("$(name) must not be empty"))
    return sources
end

function ROAffineLabel3D(source_regime_ids, reaction_order_matrix,
    output_offset; cancel_check=() -> nothing)
    sources = _ro3_source_ids(
        source_regime_ids, "affine label source_regime_ids";
        cancel_check=cancel_check)
    reaction_order_matrix isa AbstractMatrix || throw(ArgumentError(
        "reaction_order_matrix must be a matrix"))
    size(reaction_order_matrix, 2) == 3 || throw(DimensionMismatch(
        "a 3D reaction-order matrix must have three columns"))
    0 < size(reaction_order_matrix, 1) <= _RO3_MAX_OUTPUTS ||
        throw(ArgumentError(
        "a 3D reaction-order matrix must contain an output"))
    matrix = Matrix{Float64}(undef, size(reaction_order_matrix))
    for (target_row, source_row) in enumerate(
        axes(reaction_order_matrix, 1))
        _ro3_checkpoint(cancel_check)
        for (target_column, source_column) in enumerate(
            axes(reaction_order_matrix, 2))
            value = Float64(
                reaction_order_matrix[source_row, source_column])
            isfinite(value) || throw(ArgumentError(
                "3D affine labels must be finite"))
            matrix[target_row, target_column] = value
        end
    end
    _ro3_bounded_length(output_offset, "affine output offset",
        _RO3_MAX_OUTPUTS) == size(matrix, 1) || throw(DimensionMismatch(
        "affine output offset count must match reaction-order rows"))
    offset = Vector{Float64}(undef, length(output_offset))
    for (target_index, source_index) in enumerate(eachindex(output_offset))
        _ro3_checkpoint(cancel_check)
        value = Float64(output_offset[source_index])
        isfinite(value) || throw(ArgumentError(
            "3D affine labels must be finite"))
        offset[target_index] = value
    end
    return ROAffineLabel3D(
        _RO3_VALIDATED, sources, copy(matrix), copy(offset))
end

"""
One convex, full-dimensional cell candidate in physical domain coordinates.
Rows of `A` and `b` mean `A * x <= b`; the constructor maps and normalizes them
in the unit cube before invoking CDD.
"""
struct ROCellSpec3D
    key::String
    A::Matrix{Float64}
    b::Vector{Float64}
    source_regime_ids::Vector{Int}
    labels::Vector{ROAffineLabel3D}

    function ROCellSpec3D(::_RO3ValidatedToken, args...)
        new(args...)
    end
end

function ROCellSpec3D(key, A, b, source_regime_ids, labels;
    cancel_check=() -> nothing)
    key isa AbstractString || throw(ArgumentError(
        "a 3D cell specification key must be a string"))
    ncodeunits(key) <= _RO3_MAX_KEY_BYTES || throw(ArgumentError(
        "a 3D cell specification key must not exceed 1024 UTF-8 bytes"))
    normalized_key = strip(String(key))
    isempty(normalized_key) && throw(ArgumentError(
        "a 3D cell specification requires a nonempty stable key"))
    ncodeunits(normalized_key) <= _RO3_MAX_KEY_BYTES || throw(ArgumentError(
        "a 3D cell specification key must not exceed 1024 UTF-8 bytes"))
    A isa AbstractMatrix || throw(ArgumentError(
        "3D cell inequalities must be a matrix"))
    size(A, 2) == 3 || throw(DimensionMismatch(
        "3D cell inequalities must have three columns"))
    0 < size(A, 1) <= _RO3_MAX_HALFSPACES_PER_CELL ||
        throw(ArgumentError(
        "a 3D cell specification inequality count exceeds the absolute hard maximum"))
    matrix = Matrix{Float64}(undef, size(A))
    for (target_row, source_row) in enumerate(axes(A, 1))
        _ro3_checkpoint(cancel_check)
        for (target_column, source_column) in enumerate(axes(A, 2))
            value = Float64(A[source_row, source_column])
            isfinite(value) || throw(ArgumentError(
                "3D cell inequalities must be finite"))
            matrix[target_row, target_column] = value
        end
    end
    _ro3_bounded_length(b, "3D cell inequality bounds",
        _RO3_MAX_HALFSPACES_PER_CELL) == size(matrix, 1) ||
        throw(DimensionMismatch(
        "3D cell inequality bounds must match the row count"))
    bounds = Vector{Float64}(undef, length(b))
    for (target_index, source_index) in enumerate(eachindex(b))
        _ro3_checkpoint(cancel_check)
        value = Float64(b[source_index])
        isfinite(value) || throw(ArgumentError(
            "3D cell inequalities must be finite"))
        bounds[target_index] = value
    end
    sources = _ro3_source_ids(
        source_regime_ids, "cell source_regime_ids";
        cancel_check=cancel_check)
    label_count = _ro3_bounded_length(
        labels, "3D cell affine labels", _RO3_MAX_LABELS)
    label_count > 0 || throw(ArgumentError(
        "a regular 3D cell requires at least one affine label"))
    label_scalar_work = BigInt(0)
    for label in labels
        _ro3_checkpoint(cancel_check)
        label isa ROAffineLabel3D || throw(ArgumentError(
            "3D cell labels must be ROAffineLabel3D values"))
        label_scalar_work += BigInt(length(label.source_regime_ids)) +
            BigInt(length(label.output_offset)) + length(label.reaction_order_matrix)
        label_scalar_work <= _RO3_MAX_INPUT_SCALAR_WORK || throw(ArgumentError(
            "3D cell label scalar work exceeds the absolute hard maximum"))
    end
    affine_labels = ROAffineLabel3D[]
    sizehint!(affine_labels, label_count)
    for label in labels
        _ro3_checkpoint(cancel_check)
        push!(affine_labels, ROAffineLabel3D(label.source_regime_ids,
            label.reaction_order_matrix, label.output_offset;
            cancel_check=cancel_check))
    end
    output_count = size(first(affine_labels).reaction_order_matrix, 1)
    all(label -> size(label.reaction_order_matrix, 1) == output_count,
        affine_labels) || throw(DimensionMismatch(
        "all 3D cell labels must use the same output count"))
    return ROCellSpec3D(_RO3_VALIDATED, normalized_key, copy(matrix),
        copy(bounds), sources, affine_labels)
end

"""Explicit annotation for a pairwise singular or ambiguous intersection."""
struct ROInterfaceAnnotation3D
    cell_keys::NTuple{2,String}
    kind::Symbol
    reason::Symbol

    function ROInterfaceAnnotation3D(::_RO3ValidatedToken, args...)
        new(args...)
    end
end

function ROInterfaceAnnotation3D(cell_keys, kind, reason)
    _ro3_bounded_length(cell_keys, "3D interface annotation cell_keys", 2) == 2 ||
        throw(ArgumentError(
        "a 3D interface annotation requires two cell keys"))
    all(key -> key isa AbstractString &&
        ncodeunits(key) <= _RO3_MAX_KEY_BYTES, cell_keys) ||
        throw(ArgumentError(
            "3D interface annotation keys must be bounded strings"))
    keys = sort!(String.(collect(cell_keys)))
    all(key -> !isempty(strip(key)) &&
        ncodeunits(key) <= _RO3_MAX_KEY_BYTES, keys) ||
        throw(ArgumentError("3D interface annotation keys must be bounded and nonempty"))
    keys[1] != keys[2] || throw(ArgumentError(
        "a 3D interface annotation requires distinct cell keys"))
    kind in (:singular, :ambiguous) || throw(ArgumentError(
        "3D interface annotation kind must be :singular or :ambiguous"))
    reason isa Symbol || throw(ArgumentError(
        "3D interface annotation reason must be a Symbol"))
    ncodeunits(String(reason)) <= _RO3_MAX_REASON_BYTES || throw(ArgumentError(
        "3D interface annotation reason exceeds the UTF-8 hard maximum"))
    return ROInterfaceAnnotation3D(
        _RO3_VALIDATED, (keys[1], keys[2]), kind, reason)
end

"""Low/high SVD gates and independent incidence/certificate tolerances."""
struct ROCellComplex3DTolerances
    rank_absolute::Float64
    rank_relative_low::Float64
    rank_relative_high::Float64
    incidence::Float64
    certificate::Float64

    function ROCellComplex3DTolerances(::_RO3ValidatedToken, args...)
        new(args...)
    end
end

function ROCellComplex3DTolerances(;
    rank_absolute::Real=1e-12,
    rank_relative_low::Real=1e-10,
    rank_relative_high::Real=1e-8,
    incidence::Real=1e-11,
    certificate::Real=1e-8,
)
    values = Float64[rank_absolute, rank_relative_low,
        rank_relative_high, incidence, certificate]
    all(isfinite, values) && all(>(0), values) || throw(ArgumentError(
        "3D construction tolerances must be finite and positive"))
    values[2] < values[3] < 1.0 || throw(ArgumentError(
        "relative rank gates must satisfy low < high < 1"))
    values[1] <= values[4] <= values[5] || throw(ArgumentError(
        "rank_absolute <= incidence <= certificate is required"))
    return ROCellComplex3DTolerances(_RO3_VALIDATED, values...)
end

"Hard construction limits; every combinatorial population is checked explicitly."
struct ROCellComplex3DLimits
    max_cell_specs::Int
    max_halfspaces_per_cell::Int
    max_pair_checks::Int
    max_facets::Int
    max_ridges::Int
    max_vertices::Int
    max_strata::Int
    max_labels::Int
    max_source_regime_ids::Int
    max_fixed_background::Int
    max_outputs::Int
    max_total_halfspaces::Int
    max_facet_edges::Int
    max_facet_ridge_scans::Int
    max_exact_bit_work::Int
    max_annotation_work::Int
    max_identity_bytes::Int
    max_total_work::Int

    function ROCellComplex3DLimits(::_RO3ValidatedToken, args...)
        new(args...)
    end
end

function _ro3_positive_limit(raw, name::AbstractString, hard_max::Int)
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

function ROCellComplex3DLimits(;
    max_cell_specs::Integer=64,
    max_halfspaces_per_cell::Integer=256,
    max_pair_checks::Integer=2_016,
    max_facets::Integer=8_192,
    max_ridges::Integer=32_768,
    max_vertices::Integer=32_768,
    max_strata::Integer=8_192,
    max_labels::Integer=4_096,
    max_source_regime_ids::Integer=65_536,
    max_fixed_background::Integer=65_536,
    max_outputs::Integer=4_096,
    max_total_halfspaces::Integer=16_384,
    max_facet_edges::Integer=131_072,
    max_facet_ridge_scans::Integer=50_000_000,
    max_exact_bit_work::Integer=100_000_000,
    max_annotation_work::Integer=50_000_000,
    max_identity_bytes::Integer=64 * 1024 * 1024,
    max_total_work::Integer=100_000_000,
)
    return ROCellComplex3DLimits(_RO3_VALIDATED,
        _ro3_positive_limit(max_cell_specs, "max_cell_specs", 1_024),
        _ro3_positive_limit(max_halfspaces_per_cell,
            "max_halfspaces_per_cell", _RO3_MAX_HALFSPACES_PER_CELL),
        _ro3_positive_limit(max_pair_checks, "max_pair_checks", 523_776),
        _ro3_positive_limit(max_facets, "max_facets", 1_000_000),
        _ro3_positive_limit(max_ridges, "max_ridges", 4_000_000),
        _ro3_positive_limit(max_vertices, "max_vertices", 4_000_000),
        _ro3_positive_limit(max_strata, "max_strata", 1_000_000),
        _ro3_positive_limit(max_labels, "max_labels", _RO3_MAX_LABELS),
        _ro3_positive_limit(max_source_regime_ids,
            "max_source_regime_ids", _RO3_MAX_SOURCE_REGIME_IDS),
        _ro3_positive_limit(max_fixed_background,
            "max_fixed_background", _RO3_MAX_FIXED_BACKGROUND),
        _ro3_positive_limit(max_outputs, "max_outputs", _RO3_MAX_OUTPUTS),
        _ro3_positive_limit(max_total_halfspaces,
            "max_total_halfspaces", 100_000_000),
        _ro3_positive_limit(max_facet_edges,
            "max_facet_edges", 100_000_000),
        _ro3_positive_limit(max_facet_ridge_scans,
            "max_facet_ridge_scans", 1_000_000_000),
        _ro3_positive_limit(max_exact_bit_work,
            "max_exact_bit_work", _RO3_MAX_INPUT_SCALAR_WORK),
        _ro3_positive_limit(max_annotation_work,
            "max_annotation_work", 1_000_000_000),
        _ro3_positive_limit(max_identity_bytes,
            "max_identity_bytes", 1_000_000_000),
        _ro3_positive_limit(max_total_work,
            "max_total_work", _RO3_MAX_INPUT_SCALAR_WORK),
    )
end

abstract type ROCellComplex3DConstructionError <: Exception end

struct ROCellComplex3DLimitExceeded <: ROCellComplex3DConstructionError
    phase::Symbol
    requested::BigInt
    limit::Int
end

function Base.showerror(io::IO, err::ROCellComplex3DLimitExceeded)
    print(io, "3D RO face-lattice ", err.phase, " requires ",
        err.requested, ", exceeding limit=", err.limit)
end

@inline function _ro3_limit(phase::Symbol, requested::Integer, limit::Int)
    amount = BigInt(requested)
    amount <= limit || throw(
        ROCellComplex3DLimitExceeded(phase, amount, limit))
    return nothing
end

@inline function _ro3_choose3(count::Integer)
    n = BigInt(count)
    n < 3 && return BigInt(0)
    return n * (n - 1) * (n - 2) ÷ 6
end

@inline function _ro3_merge_sort_comparison_upper(count::Integer)
    n = BigInt(count)
    n <= 1 && return BigInt(0)
    levels = 0
    width = BigInt(1)
    while width < n
        width <<= 1
        levels += 1
    end
    return n * levels
end

@inline _ro3_exact(value::Float64) = _RO3Exact(value)

function _ro3_dyadic_bit_length(value::Float64)
    exact = _ro3_exact(value)
    numerator_bits = ndigits(abs(numerator(exact)); base=2)
    denominator_bits = ndigits(denominator(exact); base=2)
    return max(numerator_bits, denominator_bits)
end

@inline function _ro3_checkpoint(cancel_check)
    cancel_check()
    return nothing
end

function _ro3_cancellable_sort!(values, cancel_check; by=identity)
    return sort!(values; alg=Base.Sort.MergeSort, lt=(first, second) -> begin
        _ro3_checkpoint(cancel_check)
        isless(by(first), by(second))
    end)
end

function _ro3_cancellable_copy(values::AbstractVector{T}, cancel_check) where {T}
    copied = Vector{T}(undef, length(values))
    for index in eachindex(values)
        _ro3_checkpoint(cancel_check)
        copied[index] = values[index]
    end
    return copied
end

function _ro3_cancellable_join(values, separator::AbstractString,
    cancel_check; transform=string)
    buffer = IOBuffer()
    first_value = true
    for value in values
        _ro3_checkpoint(cancel_check)
        if first_value
            first_value = false
        else
            write(buffer, separator)
        end
        write(buffer, transform(value))
    end
    return String(take!(buffer))
end

function _ro3_copy_tolerances(tolerances::ROCellComplex3DTolerances)
    return ROCellComplex3DTolerances(
        rank_absolute=tolerances.rank_absolute,
        rank_relative_low=tolerances.rank_relative_low,
        rank_relative_high=tolerances.rank_relative_high,
        incidence=tolerances.incidence,
        certificate=tolerances.certificate,
    )
end

function _ro3_copy_limits(limits::ROCellComplex3DLimits)
    return ROCellComplex3DLimits(
        max_cell_specs=limits.max_cell_specs,
        max_halfspaces_per_cell=limits.max_halfspaces_per_cell,
        max_pair_checks=limits.max_pair_checks,
        max_facets=limits.max_facets,
        max_ridges=limits.max_ridges,
        max_vertices=limits.max_vertices,
        max_strata=limits.max_strata,
        max_labels=limits.max_labels,
        max_source_regime_ids=limits.max_source_regime_ids,
        max_fixed_background=limits.max_fixed_background,
        max_outputs=limits.max_outputs,
        max_total_halfspaces=limits.max_total_halfspaces,
        max_facet_edges=limits.max_facet_edges,
        max_facet_ridge_scans=limits.max_facet_ridge_scans,
        max_exact_bit_work=limits.max_exact_bit_work,
        max_annotation_work=limits.max_annotation_work,
        max_identity_bytes=limits.max_identity_bytes,
        max_total_work=limits.max_total_work,
    )
end

function _ro3_preflight_inputs(domain, specs, annotations, tolerances, limits,
    cancel_check)
    _ro3_checkpoint(cancel_check)
    isempty(specs) && throw(ArgumentError(
        "a 3D RO face lattice requires at least one cell specification"))
    _ro3_limit(:cell_specs, length(specs), limits.max_cell_specs)
    _ro3_limit(:fixed_background, length(domain.fixed_logqK),
        limits.max_fixed_background)
    pair_checks = BigInt(length(specs)) * BigInt(length(specs) - 1) ÷ 2
    _ro3_limit(:pair_checks, pair_checks, limits.max_pair_checks)
    _ro3_limit(:interface_annotations, length(annotations), limits.max_strata)
    annotation_work = pair_checks * length(annotations)
    _ro3_limit(:annotation_work, annotation_work,
        limits.max_annotation_work)

    total_halfspaces = BigInt(0)
    total_labels = BigInt(0)
    total_sources = BigInt(0)
    total_label_scalar_work = BigInt(0)
    total_label_numeric_work = BigInt(0)
    total_label_source_work = BigInt(0)
    maximum_label_scalar_work = BigInt(0)
    input_scalar_work = BigInt(length(domain.fixed_logqK))
    input_identity_bytes = BigInt(65_536) +
        BigInt(32) * (6 + length(domain.fixed_logqK))
    _ro3_limit(:identity_reservation, input_identity_bytes,
        limits.max_identity_bytes)
    _ro3_limit(:input_scalar_work, input_scalar_work, limits.max_total_work)
    halfspace_counts = Int[]
    sizehint!(halfspace_counts, length(specs))
    domain_bit_length = maximum(_ro3_dyadic_bit_length(value) for value in
        (domain.lower_log10..., domain.upper_log10...))
    dyadic_bit_lengths = Int[]
    sizehint!(dyadic_bit_lengths, length(specs))
    payload_row_work = BigInt(0)
    output_count = nothing
    for spec in specs
        _ro3_checkpoint(cancel_check)
        halfspaces = size(spec.A, 1)
        _ro3_limit(:halfspaces_per_cell, halfspaces,
            limits.max_halfspaces_per_cell)
        total_halfspaces += halfspaces
        payload_row_work += halfspaces
        _ro3_limit(:total_halfspaces, total_halfspaces,
            limits.max_total_halfspaces)
        push!(halfspace_counts, halfspaces)
        input_scalar_work += BigInt(4) * halfspaces +
            length(spec.source_regime_ids)
        _ro3_limit(:input_scalar_work, input_scalar_work,
            limits.max_total_work)
        input_identity_bytes += 1_024 +
            BigInt(6) * ncodeunits(spec.key) +
            BigInt(32) * (length(spec.A) + length(spec.b) +
                length(spec.source_regime_ids))
        _ro3_limit(:identity_reservation, input_identity_bytes,
            limits.max_identity_bytes)
        total_labels += length(spec.labels)
        _ro3_limit(:labels, total_labels, limits.max_labels)
        total_sources += length(spec.source_regime_ids)
        _ro3_limit(:source_regime_ids, total_sources,
            limits.max_source_regime_ids)
        spec_bit_length = domain_bit_length
        for value in spec.A
            _ro3_checkpoint(cancel_check)
            isfinite(value) || throw(ArgumentError(
                "3D cell $(spec.key) contains a non-finite inequality"))
            spec_bit_length = max(spec_bit_length,
                _ro3_dyadic_bit_length(value))
        end
        for value in spec.b
            _ro3_checkpoint(cancel_check)
            isfinite(value) || throw(ArgumentError(
                "3D cell $(spec.key) contains a non-finite inequality bound"))
            spec_bit_length = max(spec_bit_length,
                _ro3_dyadic_bit_length(value))
        end
        push!(dyadic_bit_lengths, spec_bit_length)
        for label in spec.labels
            _ro3_checkpoint(cancel_check)
            total_sources += length(label.source_regime_ids)
            _ro3_limit(:source_regime_ids, total_sources,
                limits.max_source_regime_ids)
            rows = size(label.reaction_order_matrix, 1)
            payload_row_work += rows
            label_numeric_work =
                BigInt(length(label.reaction_order_matrix)) +
                length(label.output_offset)
            label_source_work = BigInt(length(label.source_regime_ids))
            label_scalar_work = label_numeric_work + label_source_work
            total_label_scalar_work += label_scalar_work
            total_label_numeric_work += label_numeric_work
            total_label_source_work += label_source_work
            maximum_label_scalar_work = max(
                maximum_label_scalar_work, label_scalar_work)
            input_scalar_work += label_scalar_work
            _ro3_limit(:input_scalar_work, input_scalar_work,
                limits.max_total_work)
            input_identity_bytes += 512 + BigInt(32) * (
                length(label.reaction_order_matrix) +
                length(label.output_offset) +
                length(label.source_regime_ids))
            _ro3_limit(:identity_reservation, input_identity_bytes,
                limits.max_identity_bytes)
            output_count === nothing && (output_count = rows)
            _ro3_limit(:outputs, rows, limits.max_outputs)
            rows == output_count || throw(DimensionMismatch(
                "all 3D cell specifications must share one output order"))
        end
    end
    _ro3_limit(:outputs, something(output_count, 0), limits.max_outputs)
    for annotation in annotations
        _ro3_checkpoint(cancel_check)
        input_identity_bytes += 512 + BigInt(6) * (
            ncodeunits(annotation.cell_keys[1]) +
            ncodeunits(annotation.cell_keys[2]) +
            ncodeunits(String(annotation.kind)) +
            ncodeunits(String(annotation.reason)))
        _ro3_limit(:identity_reservation, input_identity_bytes,
            limits.max_identity_bytes)
    end
    _ro3_limit(:identity_reservation, input_identity_bytes,
        limits.max_identity_bytes)

    # CDD's H->V conversions can materialize up to C(h, 3) candidate
    # intersections in dimension three. Account for every cell (including the
    # six cube planes) and every pair intersection before constructing any
    # polyhedron. The exact-work bound additionally covers exact point-key
    # materialization plus merge-sort comparisons, support row x vertex scans,
    # three-column exact rank elimination, and the worst possible source-row
    # products after coincident cells merge. The latter is bounded over every
    # possible partition of the source specifications, so merging cannot make
    # the runtime exact-support search larger than its preflight receipt.
    representation_candidates = BigInt(0)
    exact_bit_work = BigInt(0)
    maximum_exact_operand_bits = BigInt(domain_bit_length)
    for (index, halfspaces) in enumerate(halfspace_counts)
        exact_rows = BigInt(halfspaces) + 6
        candidates = _ro3_choose3(exact_rows)
        _ro3_limit(:cell_vertex_candidates, candidates,
            limits.max_total_work)
        representation_candidates += candidates
        operand_bits = BigInt(dyadic_bit_lengths[index])
        maximum_exact_operand_bits = max(
            maximum_exact_operand_bits, operand_bits)
        point_key_and_sort_work = BigInt(4) * candidates +
            _ro3_merge_sort_comparison_upper(candidates)
        # For every support row, a vertex scan is followed in the worst case by
        # point-matrix materialization and three-column Gaussian elimination.
        support_and_rank_work = BigInt(24) * exact_rows * candidates
        cell_exact_work = candidates * (exact_rows + 1) +
            point_key_and_sort_work + support_and_rank_work
        exact_bit_work += cell_exact_work * operand_bits *
            (BigInt(halfspaces) + 7)
    end
    maximum_pair_candidates = BigInt(0)
    pairwise_source_row_products = BigInt(0)
    for first_index in 1:(length(halfspace_counts) - 1),
        second_index in (first_index + 1):length(halfspace_counts)
        _ro3_checkpoint(cancel_check)
        first_halfspaces = BigInt(halfspace_counts[first_index])
        second_halfspaces = BigInt(halfspace_counts[second_index])
        pair_rows = first_halfspaces + second_halfspaces + 12
        pair_candidates = _ro3_choose3(pair_rows)
        representation_candidates += pair_candidates
        maximum_pair_candidates = max(
            maximum_pair_candidates, pair_candidates)
        pairwise_source_row_products += first_halfspaces * second_halfspaces
        pair_operand_bits = BigInt(max(
            dyadic_bit_lengths[first_index],
            dyadic_bit_lengths[second_index]))
        pair_exact_work = pair_candidates * (pair_rows + 4)
        exact_bit_work += pair_exact_work * pair_operand_bits *
            (first_halfspaces + second_halfspaces + 13)
    end
    if length(specs) > 1
        # If exact-duplicate cells merge, each retained cell can own several
        # original H representations. Across any partition, the first-side row
        # scans are at most (n-1)H and the opposing row products are at most
        # sum_{i<j} h_i*h_j. Each active test is a three-coordinate exact dot
        # product; the constant eight also reserves normalized-plane checks.
        source_support_scan_rows = BigInt(length(specs) - 1) *
            total_halfspaces + pairwise_source_row_products
        opposite_support_work = BigInt(8) * (
            maximum_pair_candidates * source_support_scan_rows +
            pairwise_source_row_products)
        exact_bit_work += opposite_support_work *
            maximum_exact_operand_bits * (total_halfspaces + 7)
    end
    # Exact-cell merging sorts geometry keys, retained source keys, and source
    # H-representations. Across disjoint merge groups, each population has at
    # most n*ceil(log2(n)) merge-sort comparisons; key bytes are hard bounded.
    exact_merge_metadata_work = (
        BigInt(3) * _ro3_merge_sort_comparison_upper(length(specs)) +
        BigInt(4) * length(specs)) * _RO3_MAX_KEY_BYTES
    exact_bit_work += exact_merge_metadata_work
    _ro3_limit(:representation_candidates, representation_candidates,
        limits.max_total_work)
    _ro3_limit(:exact_bit_work, exact_bit_work,
        limits.max_exact_bit_work)

    # Each source label is rebuilt during canonicalization, then numeric labels
    # are deduplicated once per source cell and once more per merged geometry
    # group. A deduplication pass hashes each numeric key once, visits each
    # source once, sorts each accumulated source union once, and copies only the
    # retained representative. Across disjoint groups, n*ceil(log2(n)) is a
    # conservative upper bound for the sum of every cancellable MergeSort.
    # Keeping these terms explicit prevents a same-key/distinct-source
    # population from hiding quadratic work behind its small output count.
    label_sort_work = _ro3_merge_sort_comparison_upper(total_labels) *
        (maximum_label_scalar_work + 1)
    source_sort_work =
        _ro3_merge_sort_comparison_upper(total_label_source_work)
    unique_label_pass_work = BigInt(4) * total_labels +
        BigInt(2) * total_label_numeric_work +
        total_label_source_work + label_sort_work + source_sort_work + 1
    canonical_and_copy_work = BigInt(10) * total_labels +
        BigInt(7) * total_label_numeric_work +
        BigInt(4) * total_label_source_work +
        BigInt(3) * source_sort_work + label_sort_work
    label_order_work = BigInt(2) * unique_label_pass_work +
        canonical_and_copy_work +
        _ro3_merge_sort_comparison_upper(length(specs)) *
            (total_label_scalar_work + total_sources + 1)
    _ro3_limit(:label_work, label_order_work, limits.max_total_work)

    # These bounds are deliberately computed before CDD, facet, ridge, vertex,
    # or stratum populations are materialized.
    facet_upper = pair_checks + BigInt(6) * length(specs)
    stratum_upper = pair_checks + length(specs) + facet_upper
    _ro3_limit(:facet_upper_bound, facet_upper, limits.max_facets)
    _ro3_limit(:stratum_upper_bound, stratum_upper, limits.max_strata)
    support_upper = total_halfspaces + BigInt(6) * length(specs)
    facet_pair_upper = facet_upper * max(BigInt(0), facet_upper - 1) ÷ 2
    closure_work_upper = BigInt(length(specs)) * facet_upper +
        support_upper * (facet_upper + facet_pair_upper) +
        BigInt(6) * facet_pair_upper
    _ro3_limit(:closure_work, closure_work_upper, limits.max_total_work)
    work_upper = input_scalar_work + total_halfspaces + total_labels +
        total_sources + pair_checks + facet_upper + stratum_upper +
        representation_candidates + exact_bit_work + annotation_work +
        closure_work_upper + payload_row_work + label_order_work
    _ro3_limit(:preconstruction_work, work_upper, limits.max_total_work)
    return work_upper
end

function _ro3_canonical_spec(spec::ROCellSpec3D, cancel_check)
    _ro3_checkpoint(cancel_check)
    rebuilt = ROCellSpec3D(spec.key, spec.A, spec.b,
        spec.source_regime_ids, spec.labels; cancel_check=cancel_check)
    row_entries = Tuple{String,Int}[]
    sizehint!(row_entries, size(rebuilt.A, 1))
    for row in axes(rebuilt.A, 1)
        _ro3_checkpoint(cancel_check)
        row_key = string(join((_ro3_float_token(
            rebuilt.A[row, column]) for column in 1:3), ","),
            "|", _ro3_float_token(rebuilt.b[row]))
        push!(row_entries, (row_key, row))
    end
    _ro3_cancellable_sort!(row_entries, cancel_check; by=first)
    row_order = Int[]
    sizehint!(row_order, length(row_entries))
    for entry in row_entries
        _ro3_checkpoint(cancel_check)
        push!(row_order, entry[2])
    end
    label_entries = Tuple{String,ROAffineLabel3D}[]
    sizehint!(label_entries, length(rebuilt.labels))
    for label in rebuilt.labels
        _ro3_checkpoint(cancel_check)
        sort_key = string(_ro3_label_key(label, cancel_check), "|",
            _ro3_cancellable_join(label.source_regime_ids, ",",
                cancel_check))
        push!(label_entries, (sort_key, label))
    end
    _ro3_cancellable_sort!(label_entries, cancel_check; by=first)
    labels = ROAffineLabel3D[]
    sizehint!(labels, length(label_entries))
    for entry in label_entries
        _ro3_checkpoint(cancel_check)
        push!(labels, entry[2])
    end
    return ROCellSpec3D(rebuilt.key, rebuilt.A[row_order, :],
        rebuilt.b[row_order], rebuilt.source_regime_ids, labels;
        cancel_check=cancel_check)
end

function _ro3_normalize_inputs(domain::ROInputDomain3D,
    specs::AbstractVector{ROCellSpec3D},
    annotations::AbstractVector{ROInterfaceAnnotation3D}, tolerances, limits,
    cancel_check)
    preconstruction_work = _ro3_preflight_inputs(domain, specs, annotations,
        tolerances, limits, cancel_check)
    normalized_domain = ROInputDomain3D(domain.axis_indices,
        domain.lower_log10, domain.upper_log10, domain.fixed_logqK)
    normalized_tolerances = _ro3_copy_tolerances(tolerances)
    normalized_limits = _ro3_copy_limits(limits)
    normalized_specs = ROCellSpec3D[]
    sizehint!(normalized_specs, length(specs))
    for spec in specs
        push!(normalized_specs, _ro3_canonical_spec(spec, cancel_check))
    end
    _ro3_cancellable_sort!(normalized_specs, cancel_check;
        by=spec -> spec.key)
    keys = getfield.(normalized_specs, :key)
    length(unique(keys)) == length(keys) || throw(ArgumentError(
        "3D cell specification keys must be unique"))
    normalized_annotations = ROInterfaceAnnotation3D[]
    sizehint!(normalized_annotations, length(annotations))
    for annotation in annotations
        _ro3_checkpoint(cancel_check)
        rebuilt = ROInterfaceAnnotation3D(annotation.cell_keys,
            annotation.kind, annotation.reason)
        all(key -> key in keys, rebuilt.cell_keys) || throw(ArgumentError(
            "every 3D interface annotation must reference declared cell keys"))
        push!(normalized_annotations, rebuilt)
    end
    _ro3_cancellable_sort!(normalized_annotations, cancel_check;
        by=annotation -> (annotation.cell_keys, String(annotation.kind),
            String(annotation.reason)))
    return normalized_domain, normalized_specs, normalized_annotations,
        normalized_tolerances, normalized_limits, preconstruction_work
end

function _ro3_domain_preflight(domain::ROInputDomain3D, tolerances,
    cancel_check)
    _ro3_checkpoint(cancel_check)
    maximum_float = BigFloat(floatmax(Float64))
    lower = BigFloat.(collect(domain.lower_log10))
    upper = BigFloat.(collect(domain.upper_log10))
    widths_big = upper .- lower
    all(>(0), widths_big) || throw(ArgumentError(
        "3D physical domain widths must be positive"))
    widths = Float64.(widths_big)
    all(value -> isfinite(value) && value > 0, widths) || throw(ArgumentError(
        "3D physical domain widths must be positive finite Float64 values"))

    # Bound every physical primitive used below before Float64 arithmetic.
    maximum_coordinate_sum = sum(max(abs(lower[index]), abs(upper[index]))
        for index in 1:3)
    maximum_coordinate_sum <= maximum_float || throw(ArgumentError(
        "3D physical plane offsets are not representable as finite Float64"))
    maximum_edge = sqrt(sum(width^2 for width in widths_big))
    maximum_edge <= maximum_float || throw(ArgumentError(
        "3D physical edge lengths are not representable as finite Float64"))
    maximum_cross_component = 2 * max(widths_big[1] * widths_big[2],
        widths_big[1] * widths_big[3], widths_big[2] * widths_big[3])
    maximum_cross_component <= maximum_float || throw(ArgumentError(
        "3D physical facet areas/normals are not representable as finite Float64"))
    volume_big = prod(widths_big)
    volume_scale = Float64(volume_big)
    isfinite(volume_scale) && volume_scale > 0 || throw(ArgumentError(
        "3D physical cell volumes are not representable as positive finite Float64"))

    min_width, max_width = extrema(widths_big)
    construction_cap = min(
        BigFloat(_RO3_MAX_PHYSICAL_CONSTRUCTION_TOLERANCE) / max_width,
        BigFloat(_RO3_MAX_RELATIVE_CONSTRUCTION_TOLERANCE) * min_width / max_width,
    )
    maximum((BigFloat(tolerances.rank_absolute),
        BigFloat(tolerances.incidence), BigFloat(tolerances.certificate))) <=
        construction_cap || throw(ArgumentError(
        "3D construction tolerance exceeds the absolute/domain-relative hard ceiling"))

    condition_number = max_width / min_width
    publication_length = max(BigFloat(1e-12),
        BigFloat(512) * BigFloat(eps(Float64)) * max(BigFloat(1), condition_number))
    publication_length <= BigFloat(1e-6) || throw(ArgumentError(
        "3D physical domain is not resolvable under the independent Float64 publication tolerance"))
    publication = (
        length=Float64(publication_length),
        area=Float64(max(BigFloat(1e-11), 16 * publication_length)),
        volume=Float64(max(BigFloat(1e-10), 32 * publication_length)),
    )
    return (widths=Tuple(widths), volume_scale=volume_scale,
        publication=publication)
end

struct ROCellComplex3DRankAmbiguity <: ROCellComplex3DConstructionError
    context::String
    singular_values::Vector{Float64}
    low_threshold::Float64
    high_threshold::Float64
end

function Base.showerror(io::IO, err::ROCellComplex3DRankAmbiguity)
    print(io, "3D RO geometry rank is unresolved in ", err.context,
        ": singular values=", err.singular_values, " grey zone=(",
        err.low_threshold, ", ", err.high_threshold, ")")
end

struct ROCellComplex3DSliver <: ROCellComplex3DConstructionError
    context::String
    polyhedral_dimension::Int
    vertex_rank::Int
end

struct ROCellComplex3DExactDimensionMismatch <:
    ROCellComplex3DConstructionError
    context::String
    float_dimension::Int
    exact_dimension::Int
end

function Base.showerror(io::IO, err::ROCellComplex3DExactDimensionMismatch)
    print(io, "3D RO Float/exact dyadic dimension mismatch in ", err.context,
        ": Float64 dimension=", err.float_dimension,
        ", exact dimension=", err.exact_dimension)
end

function Base.showerror(io::IO, err::ROCellComplex3DSliver)
    print(io, "3D RO geometry is a rank-inconsistent sliver in ", err.context,
        ": polyhedral dimension=", err.polyhedral_dimension,
        ", vertex rank=", err.vertex_rank)
end

struct ROCellComplex3DOverlap <: ROCellComplex3DConstructionError
    cell_keys::NTuple{2,String}
end

Base.showerror(io::IO, err::ROCellComplex3DOverlap) = print(io,
    "3D RO cells have positive-volume overlap: ", err.cell_keys)

struct ROCellComplex3DClosureError <: ROCellComplex3DConstructionError
    context::String
end

Base.showerror(io::IO, err::ROCellComplex3DClosureError) = print(io,
    "3D RO face-lattice closure failed: ", err.context)

"""A zero-dimensional face with complete upward incidence."""
struct ROVertex3D
    id::Int
    coordinates::NTuple{3,Float64}
    unit_coordinates::NTuple{3,Float64}
    incident_ridge_ids::Vector{Int}
    incident_facet_ids::Vector{Int}
    incident_cell_ids::Vector{Int}
    stratum_ids::Vector{Int}

    function ROVertex3D(::_RO3ValidatedToken, args...)
        new(args...)
    end
end

"""An atomic one-dimensional face after the second common refinement."""
struct RORidge3D
    id::Int
    vertex_ids::NTuple{2,Int}
    incident_facet_ids::Vector{Int}
    incident_cell_ids::Vector{Int}
    domain_sides::Vector{Symbol}
    length::Float64
    stratum_ids::Vector{Int}

    function RORidge3D(::_RO3ValidatedToken, args...)
        new(args...)
    end
end

"""An atomic polygonal facet with complete downward and upward incidence."""
struct ROFacet3D
    id::Int
    kind::Symbol
    vertex_ids::Vector{Int}
    ridge_ids::Vector{Int}
    incident_cell_ids::Vector{Int}
    domain_side::Union{Nothing,Symbol}
    normal::NTuple{3,Float64}
    offset::Float64
    area::Float64
    stratum_ids::Vector{Int}
    ambiguous::Bool

    function ROFacet3D(::_RO3ValidatedToken, args...)
        new(args...)
    end
end

"""A full-dimensional regular cell and its complete face closure."""
struct ROCell3D
    id::Int
    source_keys::Vector{String}
    source_regime_ids::Vector{Int}
    labels::Vector{ROAffineLabel3D}
    vertex_ids::Vector{Int}
    ridge_ids::Vector{Int}
    facet_ids::Vector{Int}
    volume::Float64
    set_valued::Bool
    stratum_ids::Vector{Int}

    function ROCell3D(::_RO3ValidatedToken, args...)
        new(args...)
    end
end

"""
An explicit closure, singular, or ambiguity stratum. `support_face_ids` names
faces at `dimension`: cells, facets, ridges, or vertices for dimensions 3--0.
"""
struct ROStratum3D
    id::Int
    dimension::Int
    kind::Symbol
    support_face_ids::Vector{Int}
    vertex_ids::Vector{Int}
    incident_cell_ids::Vector{Int}
    reasons::Vector{Symbol}

    function ROStratum3D(::_RO3ValidatedToken, args...)
        new(args...)
    end
end

"""Independent checks required before the Float64 face lattice is publishable."""
struct ROCellComplex3DCertificate
    face_dimension_agreement::Bool
    no_positive_volume_overlap::Bool
    cell_facet_closure::Bool
    facet_ridge_closure::Bool
    ridge_vertex_links::Bool
    domain_side_coverage::NTuple{6,Bool}
    volume_complete::Bool
    euler_value::Int
    euler_consistent::Bool
    publishable::Bool
    exact_pair_dimension_certified::Bool
    exact_support_coverage_certified::Bool
    exact_volume_coverage_certified::Bool
    exact_pair_dimension_counts::NTuple{5,Int}
    exact_cell_volume_sum::String
    exact_domain_volume::String
    maximum_pair_overlap_unit_volume::Float64
    maximum_cell_facet_area_residual::Float64
    maximum_cell_facet_overlap_area::Float64
    maximum_domain_side_area_residual::Float64
    maximum_domain_side_overlap_area::Float64
    unit_volume_sum::Float64
    unit_volume_residual::Float64
    maximum_ridge_cell_link_residual::Int
    maximum_facet_boundary_residual::Int
    maximum_vertex_link_degree_residual::Int
    disconnected_vertex_link_count::Int
    publication_length_tolerance::Float64
    publication_area_tolerance::Float64
    publication_volume_tolerance::Float64
    evidence_scope::Symbol
    arbitrary_precision_certified::Bool
    higher_dimension_certified::Bool
    chemistry_extraction_certified::Bool

    function ROCellComplex3DCertificate(::_RO3ValidatedToken, args...)
        new(args...)
    end
end

"""
Explicit convex-affine D=3 Float64-input face lattice whose publication
decisions are recomputed over the exact dyadic values represented by those
Float64 inputs. This is not arbitrary-real or arbitrary-precision evidence.
"""
struct ROCellComplex3D
    schema_version::String
    domain::ROInputDomain3D
    cells::Vector{ROCell3D}
    facets::Vector{ROFacet3D}
    ridges::Vector{RORidge3D}
    vertices::Vector{ROVertex3D}
    strata::Vector{ROStratum3D}
    certificate::ROCellComplex3DCertificate
    has_singular_strata::Bool
    has_ambiguity::Bool
    source_specs::Vector{ROCellSpec3D}
    interface_annotations::Vector{ROInterfaceAnnotation3D}
    construction_limits::ROCellComplex3DLimits
    canonical_payload::String
    canonical_identity::String
    tolerances::ROCellComplex3DTolerances
    evidence_scope::Symbol

    function ROCellComplex3D(::_RO3ValidatedToken, args...)
        new(args...)
    end
end

mutable struct _RO3CellWork
    source_keys::Vector{String}
    source_regime_ids::Vector{Int}
    labels::Vector{ROAffineLabel3D}
    A::Matrix{Float64}
    b::Vector{Float64}
    poly
    unit_vertices::Vector{NTuple{3,Float64}}
    support_faces::Vector{Vector{NTuple{3,Float64}}}
    geometry_key::String
    set_valued::Bool
end

const _RO3Exact = Rational{BigInt}

struct _RO3ExactSourceHalfspaces
    key::String
    A::Matrix{_RO3Exact}
    b::Vector{_RO3Exact}
end

mutable struct _RO3ExactCellWork
    source_keys::Vector{String}
    source_halfspaces::Vector{_RO3ExactSourceHalfspaces}
    A::Matrix{_RO3Exact}
    b::Vector{_RO3Exact}
    poly
    vertices::Vector{NTuple{3,_RO3Exact}}
    volume::_RO3Exact
    geometry_key::String
end

struct _RO3ExactPairResult
    dimension::Int
    points::Vector{NTuple{3,_RO3Exact}}
    support_plane_key::Union{Nothing,String}
end

mutable struct _RO3FacetWork
    unit_vertices::Vector{NTuple{3,Float64}}
    incident_cell_ids::Set{Int}
    kind::Symbol
    domain_side::Union{Nothing,Symbol}
    singular_reasons::Set{Symbol}
    ambiguous_reasons::Set{Symbol}
    key::String
end

mutable struct _RO3RidgeWork
    endpoints::NTuple{2,NTuple{3,Float64}}
    incident_facet_ids::Set{Int}
end

mutable struct _RO3StratumWork
    dimension::Int
    kind::Symbol
    unit_vertices::Vector{NTuple{3,Float64}}
    incident_cell_ids::Set{Int}
    reasons::Set{Symbol}
    support_face_ids::Vector{Int}
end

@inline _ro3_add(a, b) = (a[1] + b[1], a[2] + b[2], a[3] + b[3])
@inline _ro3_sub(a, b) = (a[1] - b[1], a[2] - b[2], a[3] - b[3])
@inline _ro3_scale(a, t) = (a[1] * t, a[2] * t, a[3] * t)
@inline _ro3_dot(a, b) = a[1] * b[1] + a[2] * b[2] + a[3] * b[3]
@inline _ro3_norm(a) = hypot(a[1], a[2], a[3])
@inline _ro3_cross(a, b) = (
    a[2] * b[3] - a[3] * b[2],
    a[3] * b[1] - a[1] * b[3],
    a[1] * b[2] - a[2] * b[1],
)

function _ro3_exact_domain_halfspaces(domain, cancel_check)
    A = zeros(_RO3Exact, 6, 3)
    b = Vector{_RO3Exact}(undef, 6)
    row = 0
    for axis in 1:3
        _ro3_checkpoint(cancel_check)
        row += 1
        A[row, axis] = -1
        b[row] = -_ro3_exact(domain.lower_log10[axis])
        row += 1
        A[row, axis] = 1
        b[row] = _ro3_exact(domain.upper_log10[axis])
    end
    return A, b
end

function _ro3_exact_spec_halfspaces(spec, cancel_check)
    A = Matrix{_RO3Exact}(undef, size(spec.A))
    b = Vector{_RO3Exact}(undef, length(spec.b))
    for index in eachindex(spec.A)
        _ro3_checkpoint(cancel_check)
        A[index] = _ro3_exact(spec.A[index])
    end
    for index in eachindex(spec.b)
        _ro3_checkpoint(cancel_check)
        b[index] = _ro3_exact(spec.b[index])
    end
    return A, b
end

function _ro3_make_exact_polyhedron(A, b, cancel_check)
    _ro3_checkpoint(cancel_check)
    poly = Polyhedra.polyhedron(
        Polyhedra.hrep(A, b), CDDLib.Library(:exact))
    _ro3_checkpoint(cancel_check)
    return poly
end

function _ro3_exact_dimension_points(poly, context, cancel_check)
    _ro3_checkpoint(cancel_check)
    Polyhedra.isempty(poly) && return (-1, NTuple{3,_RO3Exact}[])
    Polyhedra.detecthlinearity!(poly)
    dimension = Polyhedra.dim(poly)
    representation = Polyhedra.MixedMatVRep(Polyhedra.vrep(poly))
    _ro3_checkpoint(cancel_check)
    size(representation.R, 1) == 0 || throw(ROCellComplex3DClosureError(
        "$(context) is unbounded in exact dyadic geometry"))
    size(representation.V, 2) == 3 || throw(ROCellComplex3DClosureError(
        "$(context) did not produce three exact coordinates"))
    points = NTuple{3,_RO3Exact}[]
    sizehint!(points, size(representation.V, 1))
    for row in axes(representation.V, 1)
        _ro3_checkpoint(cancel_check)
        push!(points, (representation.V[row, 1],
            representation.V[row, 2], representation.V[row, 3]))
    end
    return dimension, points
end

function _ro3_exact_rank(matrix::Matrix{_RO3Exact},
    cancel_check=() -> nothing)
    rows, columns = size(matrix)
    work = copy(matrix)
    rank = 0
    for column in 1:columns
        _ro3_checkpoint(cancel_check)
        pivot_row = 0
        for row in (rank + 1):rows
            _ro3_checkpoint(cancel_check)
            if !iszero(work[row, column])
                pivot_row = row
                break
            end
        end
        iszero(pivot_row) && continue
        rank += 1
        if pivot_row != rank
            work[rank, :], work[pivot_row, :] =
                copy(work[pivot_row, :]), copy(work[rank, :])
        end
        pivot_value = work[rank, column]
        work[rank, :] ./= pivot_value
        for row in 1:rows
            _ro3_checkpoint(cancel_check)
            row == rank && continue
            factor = work[row, column]
            iszero(factor) || (work[row, :] .-= factor .* work[rank, :])
        end
        rank == min(rows, columns) && break
    end
    return rank
end

function _ro3_exact_point_rank(points, cancel_check=() -> nothing)
    length(points) <= 1 && return 0
    matrix = Matrix{_RO3Exact}(undef, length(points) - 1, 3)
    anchor = first(points)
    for (row, point) in enumerate(points[2:end])
        _ro3_checkpoint(cancel_check)
        for column in 1:3
            matrix[row, column] = point[column] - anchor[column]
        end
    end
    return _ro3_exact_rank(matrix, cancel_check)
end

_ro3_exact_token(value::_RO3Exact) =
    string(numerator(value), "//", denominator(value))

_ro3_exact_point_key(point) = join(_ro3_exact_token.(collect(point)), ",")

function _ro3_exact_geometry_key(points, cancel_check=() -> nothing)
    keys = String[]
    sizehint!(keys, length(points))
    for point in points
        _ro3_checkpoint(cancel_check)
        push!(keys, _ro3_exact_point_key(point))
    end
    _ro3_cancellable_sort!(keys, cancel_check)
    return join(keys, ";")
end

function _ro3_exact_plane_key(A, b)
    pivot = findfirst(!iszero, A)
    pivot === nothing && throw(ROCellComplex3DClosureError(
        "exact support plane has a zero normal"))
    scale = A[pivot]
    values = _RO3Exact[A[index] / scale for index in eachindex(A)]
    push!(values, b / scale)
    return join(_ro3_exact_token.(values), ",")
end

function _ro3_build_exact_cell(spec, domain, cancel_check)
    raw_A, raw_b = _ro3_exact_spec_halfspaces(spec, cancel_check)
    box_A, box_b = _ro3_exact_domain_halfspaces(domain, cancel_check)
    A = vcat(raw_A, box_A)
    b = vcat(raw_b, box_b)
    poly = _ro3_make_exact_polyhedron(A, b, cancel_check)
    dimension, points = _ro3_exact_dimension_points(
        poly, "exact cell $(spec.key)", cancel_check)
    dimension == 3 || throw(ROCellComplex3DSliver(
        "exact cell $(spec.key)", dimension, dimension))
    _ro3_checkpoint(cancel_check)
    volume = _RO3Exact(Polyhedra.volume(poly))
    _ro3_checkpoint(cancel_check)
    volume > 0 || throw(ROCellComplex3DClosureError(
        "exact cell $(spec.key) has non-positive volume"))
    return _RO3ExactCellWork([spec.key],
        [_RO3ExactSourceHalfspaces(spec.key, raw_A, raw_b)], A, b, poly,
        points, volume, _ro3_exact_geometry_key(points, cancel_check))
end

function _ro3_merge_exact_cells(cells, cancel_check)
    grouped = Dict{String,Vector{_RO3ExactCellWork}}()
    for cell in cells
        _ro3_checkpoint(cancel_check)
        push!(get!(grouped, cell.geometry_key, _RO3ExactCellWork[]), cell)
    end
    merged = _RO3ExactCellWork[]
    grouped_keys = collect(keys(grouped))
    _ro3_cancellable_sort!(grouped_keys, cancel_check)
    for key in grouped_keys
        _ro3_checkpoint(cancel_check)
        group = grouped[key]
        representative = first(group)
        source_key_set = Set{String}()
        source_halfspaces = _RO3ExactSourceHalfspaces[]
        for cell in group
            _ro3_checkpoint(cancel_check)
            for source_key in cell.source_keys
                _ro3_checkpoint(cancel_check)
                push!(source_key_set, source_key)
            end
            for source in cell.source_halfspaces
                _ro3_checkpoint(cancel_check)
                push!(source_halfspaces, source)
            end
        end
        source_keys = collect(source_key_set)
        _ro3_cancellable_sort!(source_keys, cancel_check)
        _ro3_cancellable_sort!(source_halfspaces, cancel_check;
            by=source -> source.key)
        push!(merged, _RO3ExactCellWork(source_keys, source_halfspaces,
            representative.A, representative.b, representative.poly,
            representative.vertices, representative.volume, key))
    end
    return merged
end

function _ro3_exact_opposite_support(first_cell, second_cell, points,
    cancel_check)
    for first_source in first_cell.source_halfspaces
        for first_row in axes(first_source.A, 1)
            _ro3_checkpoint(cancel_check)
            first_A = view(first_source.A, first_row, :)
            first_b = first_source.b[first_row]
            first_active = true
            for point in points
                _ro3_checkpoint(cancel_check)
                if sum(first_A[column] * point[column]
                    for column in 1:3) != first_b
                    first_active = false
                    break
                end
            end
            first_active || continue
            for second_source in second_cell.source_halfspaces
                for second_row in axes(second_source.A, 1)
                    _ro3_checkpoint(cancel_check)
                    second_A = view(second_source.A, second_row, :)
                    second_b = second_source.b[second_row]
                    second_active = true
                    for point in points
                        _ro3_checkpoint(cancel_check)
                        if sum(second_A[column] * point[column]
                            for column in 1:3) != second_b
                            second_active = false
                            break
                        end
                    end
                    second_active || continue
                    pivot = findfirst(index -> !iszero(first_A[index]) &&
                        !iszero(second_A[index]), 1:3)
                    pivot === nothing && continue
                    ratio = -first_A[pivot] / second_A[pivot]
                    ratio > 0 || continue
                    all(first_A[index] == -ratio * second_A[index]
                        for index in 1:3) || continue
                    first_b == -ratio * second_b || continue
                    return _ro3_exact_plane_key(first_A, first_b)
                end
            end
        end
    end
    throw(ROCellComplex3DClosureError(
        "an exact 2D cell intersection lacks opposing original H supports"))
end

function _ro3_exact_support_plane_keys(cell, cancel_check)
    keys = Set{String}()
    for row in axes(cell.A, 1)
        _ro3_checkpoint(cancel_check)
        A = view(cell.A, row, :)
        b = cell.b[row]
        active = NTuple{3,_RO3Exact}[]
        for point in cell.vertices
            _ro3_checkpoint(cancel_check)
            sum(A[column] * point[column] for column in 1:3) == b &&
                push!(active, point)
        end
        length(active) >= 3 || continue
        _ro3_exact_point_rank(active, cancel_check) == 2 || continue
        push!(keys, _ro3_exact_plane_key(A, b))
    end
    return keys
end

function _ro3_exact_pair_certificate(cells, cancel_check)
    pair_results = Dict{Tuple{Int,Int},_RO3ExactPairResult}()
    shared_planes = [Set{String}() for _ in cells]
    dimension_counts = zeros(Int, 5)
    for first_id in 1:(length(cells) - 1),
        second_id in (first_id + 1):length(cells)
        _ro3_checkpoint(cancel_check)
        first_cell, second_cell = cells[first_id], cells[second_id]
        pair_poly = _ro3_make_exact_polyhedron(
            vcat(first_cell.A, second_cell.A),
            vcat(first_cell.b, second_cell.b), cancel_check)
        dimension, points = _ro3_exact_dimension_points(pair_poly,
            "exact intersection of cells $(first_id) and $(second_id)",
            cancel_check)
        dimension_counts[dimension + 2] += 1
        dimension == 3 && throw(ROCellComplex3DOverlap((
            join(first_cell.source_keys, "+"),
            join(second_cell.source_keys, "+"))))
        plane_key = nothing
        if dimension == 2
            plane_key = _ro3_exact_opposite_support(
                first_cell, second_cell, points, cancel_check)
            push!(shared_planes[first_id], plane_key)
            push!(shared_planes[second_id], plane_key)
        end
        pair_results[(first_id, second_id)] =
            _RO3ExactPairResult(dimension, points, plane_key)
    end

    return (
        pair_results=pair_results,
        dimension_counts=Tuple(dimension_counts),
        shared_planes=shared_planes,
        pair_dimension_certified=true,
    )
end

function _ro3_exact_coverage_certificate(cells, domain, shared_planes,
    cancel_check)
    exact_volume_sum = zero(_RO3Exact)
    for cell in cells
        _ro3_checkpoint(cancel_check)
        exact_volume_sum += cell.volume
    end
    exact_domain_volume = prod(_ro3_exact(domain.upper_log10[index]) -
        _ro3_exact(domain.lower_log10[index]) for index in 1:3)
    exact_volume_sum == exact_domain_volume || throw(
        ROCellComplex3DClosureError(
        "exact dyadic cell volumes sum to $(exact_volume_sum), expected box volume $(exact_domain_volume)"))

    box_A, box_b = _ro3_exact_domain_halfspaces(domain, cancel_check)
    domain_plane_keys = Set(_ro3_exact_plane_key(view(box_A, row, :),
        box_b[row]) for row in axes(box_A, 1))
    covered_domain_planes = Set{String}()
    for (cell_id, cell) in enumerate(cells)
        _ro3_checkpoint(cancel_check)
        for key in _ro3_exact_support_plane_keys(cell, cancel_check)
            if key in domain_plane_keys
                push!(covered_domain_planes, key)
            elseif !(key in shared_planes[cell_id])
                throw(ROCellComplex3DClosureError(
                    "exact cell $(cell_id) has an unmatched non-domain support facet"))
            end
        end
    end
    covered_domain_planes == domain_plane_keys || throw(
        ROCellComplex3DClosureError(
        "exact cells do not expose all six domain support planes"))
    return (
        exact_volume_sum=string(exact_volume_sum),
        exact_domain_volume=string(exact_domain_volume),
        support_coverage_certified=true,
        volume_coverage_certified=true,
    )
end

function _ro3_float_token(value::Float64)
    normalized = iszero(value) ? 0.0 : value
    return string(reinterpret(UInt64, normalized); base=16, pad=16)
end

function _ro3_canonical_point(point, tolerance)
    digits = clamp(ceil(Int, -log10(tolerance)) + 2, 0, 14)
    return ntuple(3) do index
        value = Float64(point[index])
        snapped = abs(value) <= tolerance ? 0.0 :
            abs(value - 1.0) <= tolerance ? 1.0 : value
        rounded = round(snapped; digits=digits)
        iszero(rounded) ? 0.0 : rounded
    end
end

_ro3_point_key(point) = join((_ro3_float_token(Float64(value))
    for value in point), ",")

function _ro3_unique_points(points, tolerance, cancel_check=() -> nothing)
    ordered = NTuple{3,Float64}[]
    sizehint!(ordered, length(points))
    for point in points
        _ro3_checkpoint(cancel_check)
        push!(ordered, _ro3_canonical_point(point, tolerance))
    end
    sort!(ordered; by=_ro3_point_key)
    result = NTuple{3,Float64}[]
    for point in ordered
        duplicate = false
        for existing in result
            _ro3_checkpoint(cancel_check)
            if _ro3_norm(_ro3_sub(existing, point)) <= tolerance
                duplicate = true
                break
            end
        end
        duplicate || push!(result, point)
    end
    return result
end

function _ro3_rank(matrix::AbstractMatrix{<:Real}, tolerances,
    context::AbstractString)
    isempty(matrix) && return 0
    singular_values = Float64.(LinearAlgebra.svdvals(Matrix{Float64}(matrix)))
    isempty(singular_values) && return 0
    scale = max(first(singular_values), 1.0)
    low = max(tolerances.rank_absolute,
        tolerances.rank_relative_low * scale)
    high = max(tolerances.rank_absolute,
        tolerances.rank_relative_high * scale)
    any(value -> low < value < high, singular_values) && throw(
        ROCellComplex3DRankAmbiguity(String(context), singular_values, low, high))
    return count(>=(high), singular_values)
end

function _ro3_point_rank(points, tolerances, context)
    length(points) <= 1 && return 0
    anchor = first(points)
    matrix = zeros(Float64, length(points) - 1, 3)
    for (row, point) in enumerate(points[2:end])
        delta = _ro3_sub(point, anchor)
        matrix[row, :] .= delta
    end
    return _ro3_rank(matrix, tolerances, context)
end

function _ro3_poly_vertices(poly, tolerances, context,
    cancel_check=() -> nothing)
    _ro3_checkpoint(cancel_check)
    representation = Polyhedra.MixedMatVRep(Polyhedra.vrep(poly))
    _ro3_checkpoint(cancel_check)
    size(representation.R, 1) == 0 || throw(ROCellComplex3DClosureError(
        "$(context) is unbounded after unit-cube clipping"))
    matrix = Matrix{Float64}(representation.V)
    size(matrix, 2) == 3 || throw(ROCellComplex3DClosureError(
        "$(context) did not produce three-coordinate vertices"))
    points = NTuple{3,Float64}[]
    sizehint!(points, size(matrix, 1))
    for row in axes(matrix, 1)
        _ro3_checkpoint(cancel_check)
        push!(points, (matrix[row, 1], matrix[row, 2], matrix[row, 3]))
    end
    return _ro3_unique_points(points, tolerances.incidence, cancel_check)
end

function _ro3_poly_dimension(poly, tolerances, context,
    cancel_check=() -> nothing)
    _ro3_checkpoint(cancel_check)
    Polyhedra.isempty(poly) && return (-1, NTuple{3,Float64}[])
    Polyhedra.detecthlinearity!(poly)
    points = _ro3_poly_vertices(poly, tolerances, context, cancel_check)
    vertex_rank = _ro3_point_rank(points, tolerances, "$(context) vertex rank")
    polyhedral_dimension = Polyhedra.dim(poly)

    representation = Polyhedra.MixedMatHRep(Polyhedra.hrep(poly))
    equality_indices = collect(representation.linset)
    equality_rank = isempty(equality_indices) ? 0 : _ro3_rank(
        Matrix{Float64}(representation.A[equality_indices, :]), tolerances,
        "$(context) supporting-hyperplane rank")
    support_dimension = 3 - equality_rank
    if vertex_rank != polyhedral_dimension || support_dimension != polyhedral_dimension
        throw(ROCellComplex3DSliver(String(context), polyhedral_dimension,
            vertex_rank))
    end
    return (polyhedral_dimension, points)
end

function _ro3_canonical_normal(normal, tolerance)
    length = _ro3_norm(normal)
    length > tolerance || throw(ROCellComplex3DClosureError(
        "could not determine a finite polygon normal"))
    normalized = _ro3_scale(normal, inv(length))
    for value in normalized
        if abs(value) > tolerance
            value < 0 && (normalized = _ro3_scale(normalized, -1.0))
            break
        end
    end
    return _ro3_canonical_point(normalized, tolerance)
end

function _ro3_polygon_normal(points, tolerance)
    length(points) >= 3 || throw(ROCellComplex3DClosureError(
        "a facet polygon requires at least three vertices"))
    anchor = first(points)
    best = (0.0, 0.0, 0.0)
    best_norm = -Inf
    for i in 2:(length(points) - 1), j in (i + 1):length(points)
        candidate = _ro3_cross(_ro3_sub(points[i], anchor),
            _ro3_sub(points[j], anchor))
        candidate_norm = _ro3_norm(candidate)
        if candidate_norm > best_norm
            best = candidate
            best_norm = candidate_norm
        end
    end
    return _ro3_canonical_normal(best, tolerance)
end

function _ro3_plane_basis(normal)
    axis_index = argmin(abs.([normal...]))
    axis = ntuple(index -> index == axis_index ? 1.0 : 0.0, 3)
    first_basis = _ro3_cross(normal, axis)
    first_basis = _ro3_scale(first_basis, inv(_ro3_norm(first_basis)))
    second_basis = _ro3_cross(normal, first_basis)
    return first_basis, second_basis
end

function _ro3_order_polygon(points, tolerances, context,
    cancel_check=() -> nothing)
    unique_points = _ro3_unique_points(
        points, tolerances.incidence, cancel_check)
    rank = _ro3_point_rank(unique_points, tolerances, "$(context) polygon rank")
    rank == 2 || throw(ROCellComplex3DClosureError(
        "$(context) polygon has vertex rank $(rank), expected 2"))
    normal = _ro3_polygon_normal(unique_points, tolerances.incidence)
    basis1, basis2 = _ro3_plane_basis(normal)
    centroid = ntuple(index ->
        sum(point[index] for point in unique_points) / length(unique_points), 3)
    sort!(unique_points; by=point -> begin
        delta = _ro3_sub(point, centroid)
        (atan(_ro3_dot(delta, basis2), _ro3_dot(delta, basis1)),
            _ro3_point_key(point))
    end)

    signed_twice_area = 0.0
    for index in eachindex(unique_points)
        _ro3_checkpoint(cancel_check)
        next_index = index == length(unique_points) ? 1 : index + 1
        a = _ro3_sub(unique_points[index], centroid)
        b = _ro3_sub(unique_points[next_index], centroid)
        signed_twice_area += _ro3_dot(_ro3_cross(a, b), normal)
    end
    signed_twice_area < 0 && reverse!(unique_points)
    start = argmin(_ro3_point_key.(unique_points))
    start != 1 && (unique_points = vcat(unique_points[start:end],
        unique_points[1:(start - 1)]))
    return unique_points
end

function _ro3_polygon_key(points)
    return join(sort!(_ro3_point_key.(points)), ";")
end

function _ro3_polygon_area(points)
    normal = _ro3_polygon_normal(points, 1e-14)
    anchor = first(points)
    twice_area = 0.0
    for index in 2:(length(points) - 1)
        twice_area += abs(_ro3_dot(_ro3_cross(
            _ro3_sub(points[index], anchor),
            _ro3_sub(points[index + 1], anchor)), normal))
    end
    return twice_area / 2
end

function _ro3_plane(points, tolerance)
    normal = _ro3_polygon_normal(points, tolerance)
    return normal, _ro3_dot(normal, first(points))
end

function _ro3_same_plane(first_points, second_points, tolerance)
    first_normal, first_offset = _ro3_plane(first_points, tolerance)
    second_normal, second_offset = _ro3_plane(second_points, tolerance)
    return _ro3_norm(_ro3_sub(first_normal, second_normal)) <= tolerance &&
        abs(first_offset - second_offset) <= tolerance
end

function _ro3_physical_point(point, domain)
    return ntuple(index -> domain.lower_log10[index] +
        (domain.upper_log10[index] - domain.lower_log10[index]) * point[index], 3)
end

function _ro3_label_key(label, cancel_check=() -> nothing)
    matrix_values = (label.reaction_order_matrix[row, column]
        for row in axes(label.reaction_order_matrix, 1)
        for column in axes(label.reaction_order_matrix, 2))
    matrix_key = _ro3_cancellable_join(matrix_values, ",", cancel_check;
        transform=_ro3_float_token)
    offset_key = _ro3_cancellable_join(label.output_offset, ",",
        cancel_check; transform=_ro3_float_token)
    return string(matrix_key, "|", offset_key)
end

mutable struct _RO3LabelAccumulator
    representative::ROAffineLabel3D
    source_regime_ids::Vector{Int}
    seen_source_regime_ids::Set{Int}
end

function _ro3_label_from_accumulator(accumulator::_RO3LabelAccumulator,
    cancel_check)
    _ro3_cancellable_sort!(accumulator.source_regime_ids, cancel_check)
    representative = accumulator.representative
    matrix = Matrix{Float64}(undef,
        size(representative.reaction_order_matrix))
    for index in eachindex(representative.reaction_order_matrix)
        _ro3_checkpoint(cancel_check)
        value = representative.reaction_order_matrix[index]
        isfinite(value) || throw(ArgumentError(
            "3D affine labels must be finite"))
        matrix[index] = value
    end
    offset = Vector{Float64}(undef, length(representative.output_offset))
    for index in eachindex(representative.output_offset)
        _ro3_checkpoint(cancel_check)
        value = representative.output_offset[index]
        isfinite(value) || throw(ArgumentError(
            "3D affine labels must be finite"))
        offset[index] = value
    end
    _ro3_checkpoint(cancel_check)
    return ROAffineLabel3D(_RO3_VALIDATED,
        accumulator.source_regime_ids, matrix, offset)
end

function _ro3_unique_labels(labels, cancel_check=() -> nothing)
    by_key = Dict{String,_RO3LabelAccumulator}()
    for label in labels
        _ro3_checkpoint(cancel_check)
        key = _ro3_label_key(label, cancel_check)
        accumulator = get!(by_key, key) do
            _RO3LabelAccumulator(label, Int[], Set{Int}())
        end
        for source_id in label.source_regime_ids
            _ro3_checkpoint(cancel_check)
            source_id > 0 || throw(ArgumentError(
                "affine label source_regime_ids must contain positive identifiers"))
            if !(source_id in accumulator.seen_source_regime_ids)
                push!(accumulator.seen_source_regime_ids, source_id)
                push!(accumulator.source_regime_ids, source_id)
            end
        end
    end
    ordered_keys = String[]
    sizehint!(ordered_keys, length(by_key))
    for key in keys(by_key)
        _ro3_checkpoint(cancel_check)
        push!(ordered_keys, key)
    end
    _ro3_cancellable_sort!(ordered_keys, cancel_check)
    unique_labels = ROAffineLabel3D[]
    sizehint!(unique_labels, length(ordered_keys))
    for key in ordered_keys
        _ro3_checkpoint(cancel_check)
        push!(unique_labels,
            _ro3_label_from_accumulator(by_key[key], cancel_check))
    end
    _ro3_checkpoint(cancel_check)
    return unique_labels
end

function _ro3_normalized_halfspaces(spec, domain, tolerances,
    cancel_check=() -> nothing)
    lower = BigFloat.(collect(domain.lower_log10))
    widths = BigFloat[BigFloat(domain.upper_log10[index]) - lower[index]
        for index in 1:3]
    matrix = Matrix{Float64}(undef, size(spec.A, 1), 3)
    bounds = Vector{Float64}(undef, size(spec.A, 1))
    for row in axes(matrix, 1)
        _ro3_checkpoint(cancel_check)
        for column in 1:3
            matrix[row, column] = Float64(BigFloat(spec.A[row, column]) *
                widths[column])
        end
        bounds[row] = Float64(BigFloat(spec.b[row]) - sum(
            BigFloat(spec.A[row, column]) * lower[column] for column in 1:3))
        all(isfinite, view(matrix, row, :)) && isfinite(bounds[row]) ||
            throw(ArgumentError(
                "3D cell $(spec.key) normalized inequalities overflow Float64"))
        row_norm = hypot(matrix[row, 1], matrix[row, 2], matrix[row, 3])
        row_norm > tolerances.rank_absolute || throw(ArgumentError(
            "3D cell $(spec.key) contains a zero-normal inequality"))
        matrix[row, :] ./= row_norm
        bounds[row] /= row_norm
    end
    cube_A = [
       -1.0  0.0  0.0
        1.0  0.0  0.0
        0.0 -1.0  0.0
        0.0  1.0  0.0
        0.0  0.0 -1.0
        0.0  0.0  1.0
    ]
    cube_b = [0.0, 1.0, 0.0, 1.0, 0.0, 1.0]
    return vcat(matrix, cube_A), vcat(bounds, cube_b)
end

function _ro3_make_polyhedron(A, b)
    return Polyhedra.polyhedron(Polyhedra.hrep(A, b), CDDLib.Library())
end

function _ro3_support_faces(A, b, vertices, tolerances, context,
    cancel_check=() -> nothing)
    by_key = Dict{String,Vector{NTuple{3,Float64}}}()
    for row in axes(A, 1)
        _ro3_checkpoint(cancel_check)
        scale = max(1.0, LinearAlgebra.norm(view(A, row, :)), abs(b[row]))
        points = [point for point in vertices if
            abs(sum(A[row, column] * point[column] for column in 1:3) - b[row]) <=
                tolerances.incidence * scale]
        length(points) >= 3 || continue
        rank = _ro3_point_rank(points, tolerances,
            "$(context) active support row $(row)")
        rank == 2 || continue
        polygon = _ro3_order_polygon(points, tolerances,
            "$(context) active support row $(row)", cancel_check)
        by_key[_ro3_polygon_key(polygon)] = polygon
    end
    faces = [by_key[key] for key in sort!(collect(keys(by_key)))]
    length(faces) >= 4 || throw(ROCellComplex3DClosureError(
        "$(context) has fewer than four full support facets"))
    return faces
end

function _ro3_build_cell_work(spec, domain, tolerances,
    cancel_check=() -> nothing, exact_geometry_key=nothing)
    _ro3_checkpoint(cancel_check)
    A, b = _ro3_normalized_halfspaces(
        spec, domain, tolerances, cancel_check)
    poly = _ro3_make_polyhedron(A, b)
    dimension, vertices = _ro3_poly_dimension(poly, tolerances,
        "cell $(spec.key)", cancel_check)
    dimension == 3 || throw(ROCellComplex3DSliver(
        "cell $(spec.key)", dimension, dimension))
    support_faces = _ro3_support_faces(A, b, vertices, tolerances,
        "cell $(spec.key)", cancel_check)
    return _RO3CellWork([spec.key],
        _ro3_cancellable_copy(spec.source_regime_ids, cancel_check),
        _ro3_unique_labels(spec.labels, cancel_check), A, b, poly, vertices,
        support_faces,
        exact_geometry_key === nothing ? _ro3_polygon_key(vertices) :
            String(exact_geometry_key), false)
end

function _ro3_merge_duplicate_cells(cells, cancel_check=() -> nothing)
    grouped = Dict{String,Vector{_RO3CellWork}}()
    for cell in cells
        _ro3_checkpoint(cancel_check)
        push!(get!(grouped, cell.geometry_key, _RO3CellWork[]), cell)
    end
    merged = _RO3CellWork[]
    geometry_keys = collect(keys(grouped))
    _ro3_cancellable_sort!(geometry_keys, cancel_check)
    for geometry_key in geometry_keys
        _ro3_checkpoint(cancel_check)
        group = grouped[geometry_key]
        first_cell = first(group)
        key_set = Set{String}()
        source_set = Set{Int}()
        for cell in group
            _ro3_checkpoint(cancel_check)
            for key in cell.source_keys
                _ro3_checkpoint(cancel_check)
                push!(key_set, key)
            end
            for source_id in cell.source_regime_ids
                _ro3_checkpoint(cancel_check)
                push!(source_set, source_id)
            end
        end
        keys = collect(key_set)
        sources = collect(source_set)
        _ro3_cancellable_sort!(keys, cancel_check)
        _ro3_cancellable_sort!(sources, cancel_check)
        labels = _ro3_unique_labels(
            Iterators.flatten((cell.labels for cell in group)), cancel_check)
        push!(merged, _RO3CellWork(keys, sources, labels, first_cell.A,
            first_cell.b, first_cell.poly, first_cell.unit_vertices,
            first_cell.support_faces, geometry_key, length(labels) > 1))
    end
    sort_entries = Tuple{String,_RO3CellWork}[]
    sizehint!(sort_entries, length(merged))
    for cell in merged
        _ro3_checkpoint(cancel_check)
        label_keys = String[]
        sizehint!(label_keys, length(cell.labels))
        for label in cell.labels
            _ro3_checkpoint(cancel_check)
            push!(label_keys, _ro3_label_key(label, cancel_check))
        end
        sort_key = string(cell.geometry_key, "|",
            _ro3_cancellable_join(cell.source_keys, ",", cancel_check), "|",
            _ro3_cancellable_join(label_keys, ";", cancel_check))
        push!(sort_entries, (sort_key, cell))
    end
    _ro3_cancellable_sort!(sort_entries, cancel_check; by=first)
    ordered = _RO3CellWork[]
    sizehint!(ordered, length(sort_entries))
    for entry in sort_entries
        _ro3_checkpoint(cancel_check)
        push!(ordered, entry[2])
    end
    return ordered
end

function _ro3_domain_side(index, upper)
    return Symbol("u", index, upper ? "_upper" : "_lower")
end

function _ro3_add_facet!(by_key, polygon, cell_ids, kind, domain_side,
    singular_reasons, ambiguous_reasons, limits=nothing,
    cancel_check=() -> nothing)
    _ro3_checkpoint(cancel_check)
    key = _ro3_polygon_key(polygon)
    if haskey(by_key, key)
        facet = by_key[key]
        union!(facet.incident_cell_ids, cell_ids)
        union!(facet.singular_reasons, singular_reasons)
        union!(facet.ambiguous_reasons, ambiguous_reasons)
        if facet.kind != kind || facet.domain_side != domain_side
            push!(facet.ambiguous_reasons, :incompatible_facet_roles)
        end
    else
        limits === nothing || _ro3_limit(
            :facets, length(by_key) + 1, limits.max_facets)
        by_key[key] = _RO3FacetWork(polygon, Set(Int.(cell_ids)), kind,
            domain_side, Set(Symbol.(singular_reasons)),
            Set(Symbol.(ambiguous_reasons)), key)
    end
    return by_key[key]
end

function _ro3_annotation_for_pair(annotations, first_keys, second_keys,
    cancel_check=() -> nothing)
    singular = Symbol[]
    ambiguous = Symbol[]
    matched = Int[]
    for (index, annotation) in enumerate(annotations)
        _ro3_checkpoint(cancel_check)
        left, right = annotation.cell_keys
        applies = (left in first_keys && right in second_keys) ||
            (right in first_keys && left in second_keys)
        applies || continue
        push!(matched, index)
        if annotation.kind === :singular
            push!(singular, annotation.reason)
        else
            push!(ambiguous, annotation.reason)
        end
    end
    return singular, ambiguous, matched
end

function _ro3_segment_endpoints(points, tolerances, context,
    cancel_check=() -> nothing)
    unique_points = _ro3_unique_points(
        points, tolerances.incidence, cancel_check)
    rank = _ro3_point_rank(unique_points, tolerances, "$(context) segment rank")
    rank == 1 || throw(ROCellComplex3DClosureError(
        "$(context) has vertex rank $(rank), expected 1"))
    best = (1, 2)
    best_length = -Inf
    for first_index in 1:(length(unique_points) - 1),
        second_index in (first_index + 1):length(unique_points)
        _ro3_checkpoint(cancel_check)
        length = _ro3_norm(_ro3_sub(unique_points[second_index],
            unique_points[first_index]))
        if length > best_length
            best = (first_index, second_index)
            best_length = length
        end
    end
    endpoints = [unique_points[best[1]], unique_points[best[2]]]
    sort!(endpoints; by=_ro3_point_key)
    return endpoints
end

function _ro3_raw_stratum!(by_key, dimension, kind, points, cell_ids, reasons,
    tolerances, cancel_check=() -> nothing)
    canonical_points = dimension == 3 ?
        _ro3_unique_points(points, tolerances.incidence, cancel_check) :
        dimension == 2 ? _ro3_order_polygon(points, tolerances,
            "stratum polygon", cancel_check) :
        dimension == 1 ? _ro3_segment_endpoints(points,
            tolerances, "pairwise closure", cancel_check) :
        _ro3_unique_points(points, tolerances.incidence, cancel_check)[1:1]
    key = string(dimension, "|", _ro3_polygon_key(canonical_points))
    if haskey(by_key, key)
        stratum = by_key[key]
        union!(stratum.incident_cell_ids, cell_ids)
        union!(stratum.reasons, reasons)
        if kind === :ambiguous ||
            (kind === :singular && stratum.kind === :closure)
            stratum.kind = kind
        end
    else
        by_key[key] = _RO3StratumWork(dimension, kind, canonical_points,
            Set(Int.(cell_ids)), Set(Symbol.(reasons)), Int[])
    end
    return by_key[key]
end

function _ro3_unit_polygon_projection(points)
    normal = _ro3_polygon_normal(points, 1e-14)
    basis1, basis2 = _ro3_plane_basis(normal)
    projected = [(Float64(_ro3_dot(point, basis1)),
        Float64(_ro3_dot(point, basis2))) for point in points]
    area = 0.0
    for index in eachindex(projected)
        next_index = index == length(projected) ? 1 : index + 1
        area += projected[index][1] * projected[next_index][2] -
            projected[index][2] * projected[next_index][1]
    end
    area < 0 && reverse!(projected)
    return projected
end

@inline _ro3_cross2(a, b) = a[1] * b[2] - a[2] * b[1]
@inline _ro3_sub2(a, b) = (a[1] - b[1], a[2] - b[2])

function _ro3_polygon2_area(points)
    length(points) < 3 && return 0.0
    twice_area = 0.0
    for index in eachindex(points)
        next_index = index == length(points) ? 1 : index + 1
        twice_area += _ro3_cross2(points[index], points[next_index])
    end
    return abs(twice_area) / 2
end

function _ro3_line_intersection2(start, stop, clip_start, clip_stop, tolerance)
    segment = _ro3_sub2(stop, start)
    clip = _ro3_sub2(clip_stop, clip_start)
    denominator = _ro3_cross2(clip, segment)
    abs(denominator) > tolerance || return stop
    parameter = _ro3_cross2(clip, _ro3_sub2(clip_start, start)) / denominator
    return (start[1] + parameter * segment[1],
        start[2] + parameter * segment[2])
end

function _ro3_convex_intersection2(subject, clip, tolerance)
    output = copy(subject)
    for clip_index in eachindex(clip)
        next_clip = clip_index == length(clip) ? 1 : clip_index + 1
        clip_start = clip[clip_index]
        clip_stop = clip[next_clip]
        input = output
        output = Tuple{Float64,Float64}[]
        isempty(input) && break
        start = input[end]
        start_inside = _ro3_cross2(_ro3_sub2(clip_stop, clip_start),
            _ro3_sub2(start, clip_start)) >= -tolerance
        for stop in input
            stop_inside = _ro3_cross2(_ro3_sub2(clip_stop, clip_start),
                _ro3_sub2(stop, clip_start)) >= -tolerance
            if stop_inside != start_inside
                push!(output, _ro3_line_intersection2(start, stop,
                    clip_start, clip_stop, tolerance))
            end
            stop_inside && push!(output, stop)
            start = stop
            start_inside = stop_inside
        end
    end
    return output
end

function _ro3_coplanar_overlap_area(first_polygon, second_polygon, tolerances)
    _ro3_same_plane(first_polygon, second_polygon,
        tolerances.certificate) || return 0.0
    normal = _ro3_polygon_normal(first_polygon, tolerances.incidence)
    basis1, basis2 = _ro3_plane_basis(normal)
    project(points) = [(Float64(_ro3_dot(point, basis1)),
        Float64(_ro3_dot(point, basis2))) for point in points]
    first_projected = project(first_polygon)
    second_projected = project(second_polygon)
    signed_area(points) = sum(points[index][1] *
        points[index == length(points) ? 1 : index + 1][2] -
        points[index][2] * points[index == length(points) ? 1 : index + 1][1]
        for index in eachindex(points))
    signed_area(first_projected) < 0 && reverse!(first_projected)
    signed_area(second_projected) < 0 && reverse!(second_projected)
    return _ro3_polygon2_area(_ro3_convex_intersection2(first_projected,
        second_projected, tolerances.incidence))
end

function _ro3_point_on_segment(point, start, stop, tolerance)
    direction = _ro3_sub(stop, start)
    length_squared = _ro3_dot(direction, direction)
    length_squared > tolerance^2 || return false
    parameter = _ro3_dot(_ro3_sub(point, start), direction) / length_squared
    -tolerance <= parameter <= 1 + tolerance || return false
    projected = _ro3_add(start, _ro3_scale(direction, parameter))
    return _ro3_norm(_ro3_sub(point, projected)) <= tolerance
end

function _ro3_cell_volume(support_faces, domain, volume_scale,
    cancel_check=() -> nothing)
    raw_point_count = sum(length, support_faces)
    raw_points = NTuple{3,Float64}[]
    sizehint!(raw_points, raw_point_count)
    for face in support_faces, point in face
        _ro3_checkpoint(cancel_check)
        push!(raw_points, point)
    end
    all_points = _ro3_unique_points(raw_points, 1e-12, cancel_check)
    centroid = ntuple(index ->
        sum(point[index] for point in all_points) / length(all_points), 3)
    unit_volume = 0.0
    for face in support_faces
        anchor = first(face)
        for index in 2:(length(face) - 1)
            _ro3_checkpoint(cancel_check)
            unit_volume += abs(_ro3_dot(_ro3_sub(anchor, centroid),
                _ro3_cross(_ro3_sub(face[index], centroid),
                    _ro3_sub(face[index + 1], centroid)))) / 6
        end
    end
    physical_volume = unit_volume * volume_scale
    isfinite(unit_volume) && isfinite(physical_volume) &&
        unit_volume > 0 && physical_volume > 0 || throw(ArgumentError(
        "3D cell volume is not a positive finite Float64 quantity"))
    return unit_volume, physical_volume
end

function _ro3_validate_cell_facets(cells, facets, publication,
    cancel_check=() -> nothing)
    validation_tolerances = (
        incidence=publication.length,
        certificate=publication.length,
    )
    maximum_area_residual = 0.0
    maximum_overlap_area = 0.0
    for (cell_id, cell) in enumerate(cells)
        _ro3_checkpoint(cancel_check)
        assigned = [facet for facet in facets if cell_id in facet.incident_cell_ids]
        for (support_index, support) in enumerate(cell.support_faces)
            _ro3_checkpoint(cancel_check)
            on_support = [facet for facet in assigned if _ro3_same_plane(
                support, facet.unit_vertices, publication.length)]
            isempty(on_support) && throw(ROCellComplex3DClosureError(
                "cell $(cell_id) support facet $(support_index) is uncovered"))
            for first_index in 1:(length(on_support) - 1),
                second_index in (first_index + 1):length(on_support)
                _ro3_checkpoint(cancel_check)
                overlap = _ro3_coplanar_overlap_area(
                    on_support[first_index].unit_vertices,
                    on_support[second_index].unit_vertices,
                    validation_tolerances)
                maximum_overlap_area = max(maximum_overlap_area, overlap)
                overlap <= publication.area || throw(
                    ROCellComplex3DClosureError(
                        "cell $(cell_id) support facet $(support_index) has overlapping atoms"))
            end
            support_area = _ro3_polygon_area(support)
            assigned_area = sum(_ro3_polygon_area(facet.unit_vertices)
                for facet in on_support)
            residual = abs(assigned_area - support_area)
            maximum_area_residual = max(maximum_area_residual, residual)
            residual <= publication.area || throw(
                ROCellComplex3DClosureError(
                    "cell $(cell_id) support facet $(support_index) area $(support_area) " *
                    "is not covered by assigned atoms $(assigned_area)"))
        end
    end
    return (valid=true, maximum_area_residual=maximum_area_residual,
        maximum_overlap_area=maximum_overlap_area)
end

function _ro3_validate_domain_sides(facets, publication,
    cancel_check=() -> nothing)
    validation_tolerances = (
        incidence=publication.length,
        certificate=publication.length,
    )
    results = Bool[]
    maximum_area_residual = 0.0
    maximum_overlap_area = 0.0
    for axis in 1:3, upper in (false, true)
        _ro3_checkpoint(cancel_check)
        side = _ro3_domain_side(axis, upper)
        polygons = [facet.unit_vertices for facet in facets if
            facet.kind === :domain && facet.domain_side === side]
        isempty(polygons) && throw(ROCellComplex3DClosureError(
            "domain side $(side) has no covering facet"))
        for first_index in 1:(length(polygons) - 1),
            second_index in (first_index + 1):length(polygons)
            _ro3_checkpoint(cancel_check)
            overlap = _ro3_coplanar_overlap_area(polygons[first_index],
                polygons[second_index], validation_tolerances)
            maximum_overlap_area = max(maximum_overlap_area, overlap)
            overlap <= publication.area || throw(
                ROCellComplex3DClosureError(
                    "domain side $(side) has overlapping facet interiors"))
        end
        total_area = sum(_ro3_polygon_area, polygons)
        residual = abs(total_area - 1.0)
        maximum_area_residual = max(maximum_area_residual, residual)
        covered = residual <= publication.area
        covered || throw(ROCellComplex3DClosureError(
            "domain side $(side) area is $(total_area), expected 1"))
        push!(results, covered)
    end
    return (coverage=Tuple(results),
        maximum_area_residual=maximum_area_residual,
        maximum_overlap_area=maximum_overlap_area)
end

function _ro3_facet_order_key(facet)
    kind_order = facet.kind === :internal ? 0 : 1
    side = facet.domain_side === nothing ? "" : String(facet.domain_side)
    return (kind_order, side, facet.key,
        join(sort!(collect(facet.incident_cell_ids)), ","))
end

function _ro3_atomic_ridges(facets, tolerances, limits, preconstruction_work,
    cancel_check=() -> nothing)
    edge_count = BigInt(sum(length(facet.unit_vertices) for facet in facets))
    _ro3_limit(:facet_edges, edge_count, limits.max_facet_edges)
    endpoint_upper = 2 * edge_count
    ridge_upper = edge_count * max(BigInt(0), endpoint_upper - 1)
    _ro3_limit(:ridge_upper_bound, ridge_upper, limits.max_ridges)
    _ro3_limit(:vertex_upper_bound, endpoint_upper, limits.max_vertices)
    scan_upper = edge_count * endpoint_upper +
        2 * edge_count * ridge_upper
    _ro3_limit(:facet_ridge_scans, scan_upper,
        limits.max_facet_ridge_scans)
    _ro3_limit(:ridge_construction_work,
        BigInt(preconstruction_work) + scan_upper,
        limits.max_total_work)
    raw_edges = Tuple{Int,NTuple{3,Float64},NTuple{3,Float64}}[]
    endpoints = NTuple{3,Float64}[]
    sizehint!(raw_edges, Int(edge_count))
    sizehint!(endpoints, Int(endpoint_upper))
    for (facet_id, facet) in enumerate(facets)
        _ro3_checkpoint(cancel_check)
        points = facet.unit_vertices
        for index in eachindex(points)
            _ro3_checkpoint(cancel_check)
            next_index = index == length(points) ? 1 : index + 1
            start, stop = points[index], points[next_index]
            push!(raw_edges, (facet_id, start, stop))
            push!(endpoints, start, stop)
        end
    end
    endpoints = _ro3_unique_points(
        endpoints, tolerances.incidence, cancel_check)
    ridges = Dict{String,_RO3RidgeWork}()
    for (facet_id, start, stop) in raw_edges
        _ro3_checkpoint(cancel_check)
        direction = _ro3_sub(stop, start)
        length_squared = _ro3_dot(direction, direction)
        points = NTuple{3,Float64}[]
        for point in endpoints
            _ro3_checkpoint(cancel_check)
            _ro3_point_on_segment(point, start, stop,
                tolerances.incidence) && push!(points, point)
        end
        sort!(points; by=point -> _ro3_dot(_ro3_sub(point, start), direction) /
            length_squared)
        for index in 1:(length(points) - 1)
            _ro3_norm(_ro3_sub(points[index + 1], points[index])) >
                tolerances.incidence || continue
            pair = sort!([points[index], points[index + 1]]; by=_ro3_point_key)
            key = string(_ro3_point_key(pair[1]), ";", _ro3_point_key(pair[2]))
            ridge = get!(ridges, key) do
                _ro3_limit(:ridges, length(ridges) + 1, limits.max_ridges)
                _RO3RidgeWork((pair[1], pair[2]), Set{Int}())
            end
            push!(ridge.incident_facet_ids, facet_id)
        end
    end
    return ([ridges[key] for key in sort!(collect(keys(ridges)))], scan_upper)
end

function _ro3_expand_facet_vertices(facet, ridge_work, vertex_id_by_key,
    tolerances, cancel_check=() -> nothing)
    expanded = Int[]
    points = facet.unit_vertices
    for index in eachindex(points)
        _ro3_checkpoint(cancel_check)
        next_index = index == length(points) ? 1 : index + 1
        start, stop = points[index], points[next_index]
        direction = _ro3_sub(stop, start)
        length_squared = _ro3_dot(direction, direction)
        edge_points = NTuple{3,Float64}[]
        for ridge in ridge_work
            _ro3_checkpoint(cancel_check)
            for endpoint in ridge.endpoints
                _ro3_point_on_segment(endpoint, start, stop,
                    tolerances.incidence) && push!(edge_points, endpoint)
            end
        end
        edge_points = _ro3_unique_points(
            edge_points, tolerances.incidence, cancel_check)
        sort!(edge_points; by=point -> _ro3_dot(_ro3_sub(point, start),
            direction) / length_squared)
        for point in edge_points[1:(end - 1)]
            push!(expanded, vertex_id_by_key[_ro3_point_key(point)])
        end
    end
    return expanded
end

function _ro3_domain_sides_for_segment(endpoints, tolerance)
    sides = Symbol[]
    for axis in 1:3, upper in (false, true)
        value = upper ? 1.0 : 0.0
        all(abs(point[axis] - value) <= tolerance for point in endpoints) &&
            push!(sides, _ro3_domain_side(axis, upper))
    end
    return sides
end

function _ro3_stratum_key(stratum)
    kind_order = stratum.kind === :ambiguous ? 0 :
        stratum.kind === :singular ? 1 : 2
    return (kind_order, -stratum.dimension,
        _ro3_polygon_key(stratum.unit_vertices),
        join(sort!(collect(stratum.incident_cell_ids)), ","),
        join(string.(sort!(collect(stratum.reasons))), ","))
end

function _ro3_float_payload(values, cancel_check=() -> nothing)
    payload = String[]
    sizehint!(payload, length(values))
    for value in values
        _ro3_checkpoint(cancel_check)
        push!(payload, _ro3_float_token(Float64(value)))
    end
    return payload
end

function _ro3_label_payload(label, cancel_check=() -> nothing)
    matrix_payload = Vector{Vector{String}}()
    sizehint!(matrix_payload, size(label.reaction_order_matrix, 1))
    for row in axes(label.reaction_order_matrix, 1)
        _ro3_checkpoint(cancel_check)
        push!(matrix_payload, _ro3_float_payload(view(
            label.reaction_order_matrix, row, :), cancel_check))
    end
    return (
        source_regime_ids=_ro3_cancellable_copy(
            label.source_regime_ids, cancel_check),
        reaction_order_matrix=matrix_payload,
        output_offset=_ro3_float_payload(label.output_offset, cancel_check),
    )
end

function _ro3_label_payloads(labels, cancel_check)
    payloads = Any[]
    sizehint!(payloads, length(labels))
    for label in labels
        _ro3_checkpoint(cancel_check)
        push!(payloads, _ro3_label_payload(label, cancel_check))
    end
    return payloads
end

function _ro3_limit_payload(limits)
    return NamedTuple{fieldnames(ROCellComplex3DLimits)}(
        Tuple(getfield(limits, name) for name in
            fieldnames(ROCellComplex3DLimits)))
end

function _ro3_tolerance_payload(tolerances)
    return (
        rank_absolute=_ro3_float_token(tolerances.rank_absolute),
        rank_relative_low=_ro3_float_token(tolerances.rank_relative_low),
        rank_relative_high=_ro3_float_token(tolerances.rank_relative_high),
        incidence=_ro3_float_token(tolerances.incidence),
        certificate=_ro3_float_token(tolerances.certificate),
    )
end

function _ro3_certificate_payload(certificate)
    return (
        face_dimension_agreement=certificate.face_dimension_agreement,
        no_positive_volume_overlap=certificate.no_positive_volume_overlap,
        cell_facet_closure=certificate.cell_facet_closure,
        facet_ridge_closure=certificate.facet_ridge_closure,
        ridge_vertex_links=certificate.ridge_vertex_links,
        domain_side_coverage=collect(certificate.domain_side_coverage),
        volume_complete=certificate.volume_complete,
        euler_value=certificate.euler_value,
        euler_consistent=certificate.euler_consistent,
        publishable=certificate.publishable,
        exact_pair_dimension_certified=
            certificate.exact_pair_dimension_certified,
        exact_support_coverage_certified=
            certificate.exact_support_coverage_certified,
        exact_volume_coverage_certified=
            certificate.exact_volume_coverage_certified,
        exact_pair_dimension_counts=collect(
            certificate.exact_pair_dimension_counts),
        exact_cell_volume_sum=certificate.exact_cell_volume_sum,
        exact_domain_volume=certificate.exact_domain_volume,
        maximum_pair_overlap_unit_volume=_ro3_float_token(
            certificate.maximum_pair_overlap_unit_volume),
        maximum_cell_facet_area_residual=_ro3_float_token(
            certificate.maximum_cell_facet_area_residual),
        maximum_cell_facet_overlap_area=_ro3_float_token(
            certificate.maximum_cell_facet_overlap_area),
        maximum_domain_side_area_residual=_ro3_float_token(
            certificate.maximum_domain_side_area_residual),
        maximum_domain_side_overlap_area=_ro3_float_token(
            certificate.maximum_domain_side_overlap_area),
        unit_volume_sum=_ro3_float_token(certificate.unit_volume_sum),
        unit_volume_residual=_ro3_float_token(
            certificate.unit_volume_residual),
        maximum_ridge_cell_link_residual=
            certificate.maximum_ridge_cell_link_residual,
        maximum_facet_boundary_residual=
            certificate.maximum_facet_boundary_residual,
        maximum_vertex_link_degree_residual=
            certificate.maximum_vertex_link_degree_residual,
        disconnected_vertex_link_count=
            certificate.disconnected_vertex_link_count,
        publication_length_tolerance=_ro3_float_token(
            certificate.publication_length_tolerance),
        publication_area_tolerance=_ro3_float_token(
            certificate.publication_area_tolerance),
        publication_volume_tolerance=_ro3_float_token(
            certificate.publication_volume_tolerance),
        evidence_scope=String(certificate.evidence_scope),
        arbitrary_precision_certified=
            certificate.arbitrary_precision_certified,
        higher_dimension_certified=certificate.higher_dimension_certified,
        chemistry_extraction_certified=
            certificate.chemistry_extraction_certified,
    )
end

function _ro3_payload_object(domain, source_specs, annotations, cells,
    facets, ridges, vertices, strata, certificate, has_singular,
    has_ambiguity, tolerances, limits, cancel_check)
    spec_payloads = Any[]
    for spec in source_specs
        _ro3_checkpoint(cancel_check)
        matrix_payload = Vector{Vector{String}}()
        sizehint!(matrix_payload, size(spec.A, 1))
        for row in axes(spec.A, 1)
            _ro3_checkpoint(cancel_check)
            push!(matrix_payload,
                _ro3_float_payload(view(spec.A, row, :), cancel_check))
        end
        push!(spec_payloads, (
            key=spec.key,
            A=matrix_payload,
            b=_ro3_float_payload(spec.b, cancel_check),
            source_regime_ids=_ro3_cancellable_copy(
                spec.source_regime_ids, cancel_check),
            labels=_ro3_label_payloads(spec.labels, cancel_check),
        ))
    end
    annotation_payloads = Any[]
    for annotation in annotations
        _ro3_checkpoint(cancel_check)
        push!(annotation_payloads, (
            cell_keys=collect(annotation.cell_keys),
            kind=String(annotation.kind),
            reason=String(annotation.reason),
        ))
    end
    cell_payloads = Any[]
    for cell in cells
        _ro3_checkpoint(cancel_check)
        push!(cell_payloads, (
            id=cell.id,
            source_keys=_ro3_cancellable_copy(
                cell.source_keys, cancel_check),
            source_regime_ids=_ro3_cancellable_copy(
                cell.source_regime_ids, cancel_check),
            labels=_ro3_label_payloads(cell.labels, cancel_check),
            vertex_ids=copy(cell.vertex_ids), ridge_ids=copy(cell.ridge_ids),
            facet_ids=copy(cell.facet_ids),
            volume=_ro3_float_token(cell.volume),
            set_valued=cell.set_valued, stratum_ids=copy(cell.stratum_ids),
        ))
    end
    facet_payloads = Any[]
    for facet in facets
        _ro3_checkpoint(cancel_check)
        push!(facet_payloads, (
            id=facet.id, kind=String(facet.kind),
            vertex_ids=copy(facet.vertex_ids), ridge_ids=copy(facet.ridge_ids),
            incident_cell_ids=copy(facet.incident_cell_ids),
            domain_side=facet.domain_side === nothing ? nothing :
                String(facet.domain_side),
            normal=_ro3_float_payload(facet.normal),
            offset=_ro3_float_token(facet.offset),
            area=_ro3_float_token(facet.area),
            stratum_ids=copy(facet.stratum_ids), ambiguous=facet.ambiguous,
        ))
    end
    ridge_payloads = Any[]
    for ridge in ridges
        _ro3_checkpoint(cancel_check)
        push!(ridge_payloads, (
            id=ridge.id, vertex_ids=collect(ridge.vertex_ids),
            incident_facet_ids=copy(ridge.incident_facet_ids),
            incident_cell_ids=copy(ridge.incident_cell_ids),
            domain_sides=String.(ridge.domain_sides),
            length=_ro3_float_token(ridge.length),
            stratum_ids=copy(ridge.stratum_ids),
        ))
    end
    vertex_payloads = Any[]
    for vertex in vertices
        _ro3_checkpoint(cancel_check)
        push!(vertex_payloads, (
            id=vertex.id,
            coordinates=_ro3_float_payload(vertex.coordinates),
            unit_coordinates=_ro3_float_payload(vertex.unit_coordinates),
            incident_ridge_ids=copy(vertex.incident_ridge_ids),
            incident_facet_ids=copy(vertex.incident_facet_ids),
            incident_cell_ids=copy(vertex.incident_cell_ids),
            stratum_ids=copy(vertex.stratum_ids),
        ))
    end
    stratum_payloads = Any[]
    for stratum in strata
        _ro3_checkpoint(cancel_check)
        push!(stratum_payloads, (
            id=stratum.id, dimension=stratum.dimension,
            kind=String(stratum.kind),
            support_face_ids=copy(stratum.support_face_ids),
            vertex_ids=copy(stratum.vertex_ids),
            incident_cell_ids=copy(stratum.incident_cell_ids),
            reasons=String.(stratum.reasons),
        ))
    end
    return (
        schema_version=RO_CELL_COMPLEX_3D_VERSION,
        evidence_scope=String(RO_CELL_COMPLEX_3D_EVIDENCE_SCOPE),
        domain=(axis_indices=collect(domain.axis_indices),
            lower_log10=_ro3_float_payload(domain.lower_log10),
            upper_log10=_ro3_float_payload(domain.upper_log10),
            fixed_logqK=_ro3_float_payload(domain.fixed_logqK)),
        source_specs=spec_payloads,
        interface_annotations=annotation_payloads,
        cells=cell_payloads, facets=facet_payloads, ridges=ridge_payloads,
        vertices=vertex_payloads, strata=stratum_payloads,
        certificate=_ro3_certificate_payload(certificate),
        has_singular_strata=has_singular,
        has_ambiguity=has_ambiguity,
        tolerances=_ro3_tolerance_payload(tolerances),
        construction_limits=_ro3_limit_payload(limits),
    )
end

@inline _ro3_json_string_reservation(value) =
    BigInt(16) + BigInt(6) * ncodeunits(String(value))

function _ro3_label_identity_reservation(label)
    float_count = BigInt(length(label.reaction_order_matrix)) +
        length(label.output_offset)
    return BigInt(512) + 32 * float_count +
        32 * length(label.source_regime_ids)
end

function _ro3_identity_reservation(domain, source_specs, annotations, cells,
    facets, ridges, vertices, strata, certificate, tolerances, limits,
    cancel_check)
    # Per-scalar constants include JSON quotes, commas, field names, and a
    # margin for the longest decimal Int. Strings use the worst six-byte JSON
    # escape expansion for every UTF-8 code unit.
    bytes = BigInt(65_536) +
        BigInt(32) * (6 + length(domain.fixed_logqK)) +
        BigInt(64) * fieldcount(ROCellComplex3DLimits) +
        BigInt(64) * fieldcount(ROCellComplex3DTolerances) +
        BigInt(128) * fieldcount(ROCellComplex3DCertificate)
    bytes += _ro3_json_string_reservation(certificate.exact_cell_volume_sum) +
        _ro3_json_string_reservation(certificate.exact_domain_volume)
    for spec in source_specs
        _ro3_checkpoint(cancel_check)
        bytes += 1_024 + _ro3_json_string_reservation(spec.key)
        bytes += BigInt(32) * (length(spec.A) + length(spec.b) +
            length(spec.source_regime_ids))
        for label in spec.labels
            _ro3_checkpoint(cancel_check)
            bytes += _ro3_label_identity_reservation(label)
        end
    end
    for annotation in annotations
        _ro3_checkpoint(cancel_check)
        bytes += 512 + _ro3_json_string_reservation(annotation.cell_keys[1]) +
            _ro3_json_string_reservation(annotation.cell_keys[2]) +
            _ro3_json_string_reservation(annotation.kind) +
            _ro3_json_string_reservation(annotation.reason)
    end
    for cell in cells
        _ro3_checkpoint(cancel_check)
        bytes += 1_024 + BigInt(32) * (1 + length(cell.source_regime_ids) +
            length(cell.vertex_ids) + length(cell.ridge_ids) +
            length(cell.facet_ids) + length(cell.stratum_ids))
        for key in cell.source_keys
            _ro3_checkpoint(cancel_check)
            bytes += _ro3_json_string_reservation(key)
        end
        for label in cell.labels
            _ro3_checkpoint(cancel_check)
            bytes += _ro3_label_identity_reservation(label)
        end
    end
    for facet in facets
        _ro3_checkpoint(cancel_check)
        bytes += 1_024 + BigInt(32) * (6 + length(facet.vertex_ids) +
            length(facet.ridge_ids) + length(facet.incident_cell_ids) +
            length(facet.stratum_ids))
        bytes += _ro3_json_string_reservation(facet.kind)
        facet.domain_side === nothing ||
            (bytes += _ro3_json_string_reservation(facet.domain_side))
    end
    for ridge in ridges
        _ro3_checkpoint(cancel_check)
        bytes += 768 + BigInt(32) * (4 + length(ridge.incident_facet_ids) +
            length(ridge.incident_cell_ids) + length(ridge.stratum_ids))
        for side in ridge.domain_sides
            _ro3_checkpoint(cancel_check)
            bytes += _ro3_json_string_reservation(side)
        end
    end
    for vertex in vertices
        _ro3_checkpoint(cancel_check)
        bytes += 768 + BigInt(32) * (7 + length(vertex.incident_ridge_ids) +
            length(vertex.incident_facet_ids) +
            length(vertex.incident_cell_ids) + length(vertex.stratum_ids))
    end
    for stratum in strata
        _ro3_checkpoint(cancel_check)
        bytes += 768 + BigInt(32) * (2 + length(stratum.support_face_ids) +
            length(stratum.vertex_ids) + length(stratum.incident_cell_ids))
        bytes += _ro3_json_string_reservation(stratum.kind)
        for reason in stratum.reasons
            _ro3_checkpoint(cancel_check)
            bytes += _ro3_json_string_reservation(reason)
        end
    end
    return bytes
end

function _ro3_payload_row_work(domain, source_specs, annotations, cells,
    facets, ridges, vertices, strata, cancel_check)
    work = BigInt(6 + length(domain.fixed_logqK) + length(annotations))
    for spec in source_specs
        _ro3_checkpoint(cancel_check)
        work += length(spec.A) + length(spec.b) +
            length(spec.source_regime_ids) + 1
        for label in spec.labels
            _ro3_checkpoint(cancel_check)
            work += length(label.reaction_order_matrix) +
                length(label.output_offset) + length(label.source_regime_ids)
        end
    end
    for cell in cells
        _ro3_checkpoint(cancel_check)
        work += 3 + length(cell.source_keys) +
            length(cell.source_regime_ids) + length(cell.vertex_ids) +
            length(cell.ridge_ids) + length(cell.facet_ids) +
            length(cell.stratum_ids)
        for label in cell.labels
            _ro3_checkpoint(cancel_check)
            work += length(label.reaction_order_matrix) +
                length(label.output_offset) + length(label.source_regime_ids)
        end
    end
    for facet in facets
        _ro3_checkpoint(cancel_check)
        work += 8 + length(facet.vertex_ids) + length(facet.ridge_ids) +
            length(facet.incident_cell_ids) + length(facet.stratum_ids)
    end
    for ridge in ridges
        _ro3_checkpoint(cancel_check)
        work += 5 + length(ridge.incident_facet_ids) +
            length(ridge.incident_cell_ids) + length(ridge.domain_sides) +
            length(ridge.stratum_ids)
    end
    for vertex in vertices
        _ro3_checkpoint(cancel_check)
        work += 7 + length(vertex.incident_ridge_ids) +
            length(vertex.incident_facet_ids) +
            length(vertex.incident_cell_ids) + length(vertex.stratum_ids)
    end
    for stratum in strata
        _ro3_checkpoint(cancel_check)
        work += 4 + length(stratum.support_face_ids) +
            length(stratum.vertex_ids) + length(stratum.incident_cell_ids) +
            length(stratum.reasons)
    end
    return work
end

function _ro3_canonical_payload(domain, source_specs, annotations, cells,
    facets, ridges, vertices, strata, certificate, has_singular,
    has_ambiguity, tolerances, limits, cancel_check, work_base=BigInt(0))
    estimate = _ro3_identity_reservation(domain, source_specs, annotations,
        cells, facets, ridges, vertices, strata, certificate, tolerances,
        limits, cancel_check)
    _ro3_limit(:identity_reservation, estimate, limits.max_identity_bytes)
    payload_work = _ro3_payload_row_work(domain, source_specs, annotations,
        cells, facets, ridges, vertices, strata, cancel_check)
    _ro3_limit(:payload_row_work, BigInt(work_base) + payload_work,
        limits.max_total_work)
    object = _ro3_payload_object(domain, source_specs, annotations, cells,
        facets, ridges, vertices, strata, certificate, has_singular,
        has_ambiguity, tolerances, limits, cancel_check)
    _ro3_checkpoint(cancel_check)
    payload = String(JSON3.write(object))
    _ro3_checkpoint(cancel_check)
    _ro3_limit(:identity_bytes, ncodeunits(payload), limits.max_identity_bytes)
    return payload
end

_ro3_payload_identity(payload::String) =
    "sha256:" * bytes2hex(SHA.sha256(codeunits(payload)))

"""
    build_ro_cell_complex_3d(domain, specs; annotations=[], tolerances=...)

Build an explicit convex-affine D=3 Float64-input enumerated-consistency face
lattice. Constraints are normalized in a unit cube for the Float64 lattice,
while pair dimensions, opposing supports, and whole-domain volume/coverage are
independently decided after converting every declared Float64 to its exact
binary dyadic rational. Pairwise cell intersections create atomic internal
facets, and facet edges are refined again before publication. Positive-volume
overlap, uncovered cell faces, volume gaps, slivers, or Float64 SVD grey-zone
ranks fail closed. Dyadic exactness applies only to the supplied Float64 values;
this does not certify arbitrary-real precision, D >= 4, or extraction of the
declared cells from chemistry.
"""
function build_ro_cell_complex_3d(domain::ROInputDomain3D,
    specs::AbstractVector{ROCellSpec3D};
    annotations::AbstractVector{ROInterfaceAnnotation3D}=ROInterfaceAnnotation3D[],
    tolerances::ROCellComplex3DTolerances=ROCellComplex3DTolerances(),
    limits::ROCellComplex3DLimits=ROCellComplex3DLimits(),
    cancel_check=() -> nothing,
)
    return _ro3_build_ro_cell_complex_3d(domain, specs, annotations,
        tolerances, limits, cancel_check, true)
end

function _ro3_build_ro_cell_complex_3d(domain, specs, annotations,
    tolerances, limits, cancel_check, validate_result::Bool)
    domain, specs, annotations, tolerances, limits, preconstruction_work =
        _ro3_normalize_inputs(domain, specs, annotations, tolerances, limits,
            cancel_check)
    physical = _ro3_domain_preflight(domain, tolerances, cancel_check)
    exact_unmerged = _RO3ExactCellWork[]
    sizehint!(exact_unmerged, length(specs))
    exact_key_by_spec = Dict{String,String}()
    for spec in specs
        _ro3_checkpoint(cancel_check)
        exact_cell = _ro3_build_exact_cell(spec, domain, cancel_check)
        push!(exact_unmerged, exact_cell)
        exact_key_by_spec[spec.key] = exact_cell.geometry_key
    end
    exact_cells = _ro3_merge_exact_cells(exact_unmerged, cancel_check)
    cell_work = _RO3CellWork[]
    sizehint!(cell_work, length(specs))
    for spec in specs
        _ro3_checkpoint(cancel_check)
        push!(cell_work,
            _ro3_build_cell_work(spec, domain, tolerances, cancel_check,
                exact_key_by_spec[spec.key]))
    end
    cells = _ro3_merge_duplicate_cells(cell_work, cancel_check)
    length(cells) == length(exact_cells) || throw(
        ROCellComplex3DClosureError(
        "Float and exact dyadic cell merging produced different populations"))
    for cell_id in eachindex(cells)
        cells[cell_id].geometry_key == exact_cells[cell_id].geometry_key ||
            throw(ROCellComplex3DClosureError(
                "Float and exact cell geometry keys disagree at cell $(cell_id)"))
    end
    exact_pair_certificate = _ro3_exact_pair_certificate(
        exact_cells, cancel_check)
    facets_by_key = Dict{String,_RO3FacetWork}()
    raw_strata = Dict{String,_RO3StratumWork}()
    matched_annotations = Set{Int}()

    # Pairwise common refinement is the only source of internal facets.
    for first_id in 1:(length(cells) - 1), second_id in (first_id + 1):length(cells)
        _ro3_checkpoint(cancel_check)
        first_cell, second_cell = cells[first_id], cells[second_id]
        intersection = Polyhedra.intersect(first_cell.poly, second_cell.poly)
        dimension, points = _ro3_poly_dimension(intersection, tolerances,
            "intersection of cells $(first_id) and $(second_id)", cancel_check)
        exact_pair = exact_pair_certificate.pair_results[(first_id, second_id)]
        dimension == exact_pair.dimension || throw(
            ROCellComplex3DExactDimensionMismatch(
                "intersection of cells $(first_id) and $(second_id)",
                dimension, exact_pair.dimension))
        dimension < 0 && continue
        singular_reasons, ambiguous_reasons, matched = _ro3_annotation_for_pair(
            annotations, first_cell.source_keys, second_cell.source_keys,
            cancel_check)
        union!(matched_annotations, matched)
        if dimension == 3
            throw(ROCellComplex3DOverlap((join(first_cell.source_keys, "+"),
                join(second_cell.source_keys, "+"))))
        elseif dimension == 2
            polygon = _ro3_order_polygon(points, tolerances,
                "intersection of cells $(first_id) and $(second_id)",
                cancel_check)
            facet = _ro3_add_facet!(facets_by_key, polygon,
                (first_id, second_id), :internal, nothing,
                singular_reasons, ambiguous_reasons, limits, cancel_check)
            if !isempty(singular_reasons)
                _ro3_raw_stratum!(raw_strata, 2, :singular, polygon,
                    (first_id, second_id), singular_reasons, tolerances,
                    cancel_check)
            end
            if !isempty(ambiguous_reasons)
                _ro3_raw_stratum!(raw_strata, 2, :ambiguous, polygon,
                    (first_id, second_id), ambiguous_reasons, tolerances,
                    cancel_check)
            end
            length(facet.incident_cell_ids) > 2 &&
                push!(facet.ambiguous_reasons, :nonmanifold_internal_facet)
        else
            kind = !isempty(ambiguous_reasons) ? :ambiguous :
                !isempty(singular_reasons) ? :singular : :closure
            reasons = isempty(vcat(singular_reasons, ambiguous_reasons)) ?
                [:pairwise_closure] : vcat(singular_reasons, ambiguous_reasons)
            _ro3_raw_stratum!(raw_strata, dimension, kind, points,
                (first_id, second_id), reasons, tolerances, cancel_check)
        end
    end
    length(matched_annotations) == length(annotations) || throw(
        ROCellComplex3DClosureError(
            "an interface annotation did not intersect its declared cells"))
    exact_coverage_certificate = _ro3_exact_coverage_certificate(exact_cells,
        domain, exact_pair_certificate.shared_planes, cancel_check)
    exact_certificate = merge(
        exact_pair_certificate, exact_coverage_certificate)

    # Every cell/domain-plane intersection is an atomic domain facet.
    for (cell_id, cell) in enumerate(cells)
        for axis in 1:3, upper in (false, true)
            _ro3_checkpoint(cancel_check)
            value = upper ? 1.0 : 0.0
            points = [point for point in cell.unit_vertices if
                abs(point[axis] - value) <= tolerances.incidence]
            length(points) >= 3 || continue
            rank = _ro3_point_rank(points, tolerances,
                "cell $(cell_id) domain-side rank")
            rank == 2 || continue
            polygon = _ro3_order_polygon(points, tolerances,
                "cell $(cell_id) domain facet", cancel_check)
            _ro3_add_facet!(facets_by_key, polygon, (cell_id,), :domain,
                _ro3_domain_side(axis, upper), Symbol[], Symbol[], limits,
                cancel_check)
        end
    end

    facet_work = collect(values(facets_by_key))
    _ro3_limit(:facets, length(facet_work), limits.max_facets)
    sort!(facet_work; by=_ro3_facet_order_key)
    cell_facet_validation = _ro3_validate_cell_facets(
        cells, facet_work, physical.publication, cancel_check)
    domain_validation = _ro3_validate_domain_sides(
        facet_work, physical.publication, cancel_check)
    domain_side_coverage = domain_validation.coverage

    unit_volumes = Float64[]
    physical_volumes = Float64[]
    for cell in cells
        _ro3_checkpoint(cancel_check)
        unit_volume, physical_volume = _ro3_cell_volume(cell.support_faces,
            domain, physical.volume_scale, cancel_check)
        push!(unit_volumes, unit_volume)
        push!(physical_volumes, physical_volume)
    end
    unit_volume_sum = sum(unit_volumes)
    unit_volume_residual = abs(unit_volume_sum - 1.0)

    ridge_work, ridge_scan_work = _ro3_atomic_ridges(
        facet_work, tolerances, limits, preconstruction_work, cancel_check)
    _ro3_limit(:ridges, length(ridge_work), limits.max_ridges)
    raw_ridge_points = NTuple{3,Float64}[]
    sizehint!(raw_ridge_points, 2 * length(ridge_work))
    for ridge in ridge_work, point in ridge.endpoints
        _ro3_checkpoint(cancel_check)
        push!(raw_ridge_points, point)
    end
    all_points = _ro3_unique_points(
        raw_ridge_points, tolerances.incidence, cancel_check)
    _ro3_limit(:vertices, length(all_points), limits.max_vertices)
    sort!(all_points; by=_ro3_point_key)
    vertex_id_by_key = Dict(_ro3_point_key(point) => id
        for (id, point) in enumerate(all_points))

    facet_vertex_ids = Vector{Vector{Int}}()
    sizehint!(facet_vertex_ids, length(facet_work))
    for facet in facet_work
        _ro3_checkpoint(cancel_check)
        push!(facet_vertex_ids, _ro3_expand_facet_vertices(facet, ridge_work,
            vertex_id_by_key, tolerances, cancel_check))
    end
    ridge_vertex_ids = [begin
        first_id = vertex_id_by_key[_ro3_point_key(ridge.endpoints[1])]
        second_id = vertex_id_by_key[_ro3_point_key(ridge.endpoints[2])]
        first_id < second_id ? (first_id, second_id) : (second_id, first_id)
    end for ridge in ridge_work]
    facet_ridge_ids = [sort!([ridge_id for (ridge_id, ridge) in
        enumerate(ridge_work) if facet_id in ridge.incident_facet_ids])
        for facet_id in eachindex(facet_work)]

    # Add explicit ambiguity strata for coincident, set-valued cells and facets.
    for (cell_id, cell) in enumerate(cells)
        _ro3_checkpoint(cancel_check)
        cell.set_valued || continue
        _ro3_raw_stratum!(raw_strata, 3, :ambiguous, cell.unit_vertices,
            (cell_id,), (:coincident_distinct_affine_labels,), tolerances,
            cancel_check)
    end
    for (facet_id, facet) in enumerate(facet_work)
        _ro3_checkpoint(cancel_check)
        isempty(facet.ambiguous_reasons) && continue
        stratum = _ro3_raw_stratum!(raw_strata, 2, :ambiguous,
            facet.unit_vertices, facet.incident_cell_ids,
            collect(facet.ambiguous_reasons), tolerances, cancel_check)
        stratum.support_face_ids = [facet_id]
    end

    # Resolve every raw stratum onto atomic faces at its own dimension.
    for stratum in values(raw_strata)
        _ro3_checkpoint(cancel_check)
        if stratum.dimension == 3
            stratum.support_face_ids = sort!(collect(stratum.incident_cell_ids))
        elseif stratum.dimension == 2
            if isempty(stratum.support_face_ids)
                key = _ro3_polygon_key(stratum.unit_vertices)
                facet_id = findfirst(facet -> facet.key == key, facet_work)
                facet_id === nothing && throw(ROCellComplex3DClosureError(
                    "a 2D stratum has no atomic facet support"))
                stratum.support_face_ids = [facet_id]
            end
        elseif stratum.dimension == 1
            start, stop = stratum.unit_vertices
            supports = Int[]
            for (ridge_id, ridge) in enumerate(ridge_work)
                _ro3_checkpoint(cancel_check)
                if all(point -> _ro3_point_on_segment(point, start, stop,
                    tolerances.incidence), ridge.endpoints)
                    push!(supports, ridge_id)
                end
            end
            isempty(supports) && throw(ROCellComplex3DClosureError(
                "a 1D stratum has no atomic ridge support"))
            stratum.support_face_ids = sort!(supports)
        else
            key = _ro3_point_key(only(stratum.unit_vertices))
            haskey(vertex_id_by_key, key) || throw(ROCellComplex3DClosureError(
                "a 0D stratum has no atomic vertex support"))
            stratum.support_face_ids = [vertex_id_by_key[key]]
        end
    end
    stratum_work = collect(values(raw_strata))
    _ro3_limit(:strata, length(stratum_work), limits.max_strata)
    sort!(stratum_work; by=_ro3_stratum_key)

    stratum_vertex_ids = Vector{Vector{Int}}()
    for stratum in stratum_work
        _ro3_checkpoint(cancel_check)
        if stratum.dimension == 3
            ids = sort!(unique(vcat((facet_vertex_ids[facet_id]
                for cell_id in stratum.support_face_ids for facet_id in
                findall(facet -> cell_id in facet.incident_cell_ids,
                    facet_work))...)))
            push!(stratum_vertex_ids, ids)
        elseif stratum.dimension == 2
            push!(stratum_vertex_ids,
                sort!(unique(vcat(facet_vertex_ids[stratum.support_face_ids]...))))
        elseif stratum.dimension == 1
            push!(stratum_vertex_ids, sort!(unique(vcat((collect(
                ridge_vertex_ids[ridge_id]) for ridge_id in
                stratum.support_face_ids)...))))
        else
            push!(stratum_vertex_ids, copy(stratum.support_face_ids))
        end
    end

    cell_strata = [Int[] for _ in cells]
    facet_strata = [Int[] for _ in facet_work]
    ridge_strata = [Int[] for _ in ridge_work]
    vertex_strata = [Int[] for _ in all_points]
    for (stratum_id, stratum) in enumerate(stratum_work)
        _ro3_checkpoint(cancel_check)
        for cell_id in stratum.incident_cell_ids
            push!(cell_strata[cell_id], stratum_id)
        end
        if stratum.dimension == 3
            for cell_id in stratum.support_face_ids
                for facet_id in eachindex(facet_work)
                    cell_id in facet_work[facet_id].incident_cell_ids &&
                        push!(facet_strata[facet_id], stratum_id)
                end
            end
        elseif stratum.dimension == 2
            append!(facet_strata[only(stratum.support_face_ids)], [stratum_id])
        elseif stratum.dimension == 1
            for ridge_id in stratum.support_face_ids
                push!(ridge_strata[ridge_id], stratum_id)
                for facet_id in ridge_work[ridge_id].incident_facet_ids
                    push!(facet_strata[facet_id], stratum_id)
                end
            end
        end
        for vertex_id in stratum_vertex_ids[stratum_id]
            push!(vertex_strata[vertex_id], stratum_id)
        end
    end

    ridges = RORidge3D[]
    for (ridge_id, ridge) in enumerate(ridge_work)
        _ro3_checkpoint(cancel_check)
        facet_ids = sort!(collect(ridge.incident_facet_ids))
        cell_ids = sort!(unique(vcat((collect(
            facet_work[facet_id].incident_cell_ids) for facet_id in facet_ids)...)))
        physical_endpoints = Tuple(_ro3_physical_point(point, domain)
            for point in ridge.endpoints)
        length = _ro3_norm(_ro3_sub(physical_endpoints[2], physical_endpoints[1]))
        isfinite(length) && length > 0 || throw(ArgumentError(
            "3D ridge length is not a positive finite Float64 quantity"))
        push!(ridges, RORidge3D(_RO3_VALIDATED,
            ridge_id, ridge_vertex_ids[ridge_id],
            facet_ids, cell_ids, _ro3_domain_sides_for_segment(ridge.endpoints,
                tolerances.incidence), length,
            sort!(unique(ridge_strata[ridge_id]))))
    end

    facets = ROFacet3D[]
    for (facet_id, facet) in enumerate(facet_work)
        _ro3_checkpoint(cancel_check)
        physical_polygon = [_ro3_physical_point(point, domain)
            for point in facet.unit_vertices]
        normal, offset = _ro3_plane(physical_polygon, tolerances.incidence)
        area = _ro3_polygon_area(physical_polygon)
        all(isfinite, normal) && isfinite(offset) && isfinite(area) && area > 0 ||
            throw(ArgumentError(
                "3D facet plane/area is not representable as finite Float64"))
        push!(facets, ROFacet3D(_RO3_VALIDATED, facet_id, facet.kind,
            facet_vertex_ids[facet_id], facet_ridge_ids[facet_id],
            sort!(collect(facet.incident_cell_ids)), facet.domain_side,
            normal, offset, area,
            sort!(unique(facet_strata[facet_id])),
            !isempty(facet.ambiguous_reasons)))
    end

    cell_objects = ROCell3D[]
    for (cell_id, cell) in enumerate(cells)
        _ro3_checkpoint(cancel_check)
        facet_ids = findall(facet -> cell_id in facet.incident_cell_ids,
            facet_work)
        ridge_ids = sort!(unique(vcat(facet_ridge_ids[facet_ids]...)))
        vertex_ids = sort!(unique(vcat(facet_vertex_ids[facet_ids]...)))
        labels = ROAffineLabel3D[]
        sizehint!(labels, length(cell.labels))
        for label in cell.labels
            _ro3_checkpoint(cancel_check)
            push!(labels, ROAffineLabel3D(label.source_regime_ids,
                label.reaction_order_matrix, label.output_offset;
                cancel_check=cancel_check))
        end
        push!(cell_objects, ROCell3D(_RO3_VALIDATED,
            cell_id, _ro3_cancellable_copy(cell.source_keys, cancel_check),
            _ro3_cancellable_copy(cell.source_regime_ids, cancel_check),
            labels, vertex_ids,
            ridge_ids, facet_ids, physical_volumes[cell_id], cell.set_valued,
            sort!(unique(cell_strata[cell_id]))))
    end

    vertex_objects = ROVertex3D[]
    for (vertex_id, point) in enumerate(all_points)
        _ro3_checkpoint(cancel_check)
        ridge_ids = findall(ridge -> vertex_id in ridge.vertex_ids, ridges)
        facet_ids = sort!(unique(vcat((ridges[ridge_id].incident_facet_ids
            for ridge_id in ridge_ids)...)))
        cell_ids = sort!(unique(vcat((ridges[ridge_id].incident_cell_ids
            for ridge_id in ridge_ids)...)))
        physical_point = _ro3_physical_point(point, domain)
        all(isfinite, physical_point) || throw(ArgumentError(
            "3D vertex coordinates are not finite Float64 values"))
        push!(vertex_objects, ROVertex3D(_RO3_VALIDATED, vertex_id,
            physical_point, point, ridge_ids, facet_ids,
            cell_ids, sort!(unique(vertex_strata[vertex_id]))))
    end

    strata = ROStratum3D[]
    for (stratum_id, stratum) in enumerate(stratum_work)
        _ro3_checkpoint(cancel_check)
        push!(strata, ROStratum3D(_RO3_VALIDATED,
            stratum_id, stratum.dimension, stratum.kind,
            copy(stratum.support_face_ids), stratum_vertex_ids[stratum_id],
            sort!(collect(stratum.incident_cell_ids)),
            sort!(collect(stratum.reasons))))
    end

    # Complete closure checks. Euler is deliberately the last, not the only,
    # certificate component.
    maximum_facet_boundary_residual = maximum((max(
        abs(length(facet.vertex_ids) - length(facet.ridge_ids)),
        max(0, 3 - length(facet.vertex_ids))) for facet in facets); init=0)
    facet_ridge_closure = all(facet -> length(facet.vertex_ids) ==
        length(facet.ridge_ids) >= 3, facets)
    facet_ridge_closure || throw(ROCellComplex3DClosureError(
        "an atomic facet does not have one ridge per boundary edge"))
    ridge_vertex_links = all(ridge -> length(ridge.incident_facet_ids) >= 2 &&
        ridge.vertex_ids[1] != ridge.vertex_ids[2], ridges)
    ridge_vertex_links || throw(ROCellComplex3DClosureError(
        "a ridge lacks two distinct vertices or two incident facets"))
    maximum_ridge_cell_link_residual = 0
    maximum_vertex_link_degree_residual = 0
    disconnected_vertex_link_count = 0
    for cell in cell_objects
        _ro3_checkpoint(cancel_check)
        for ridge_id in cell.ridge_ids
            _ro3_checkpoint(cancel_check)
            incident_count = count(facet_id -> facet_id in cell.facet_ids,
                ridges[ridge_id].incident_facet_ids)
            maximum_ridge_cell_link_residual = max(
                maximum_ridge_cell_link_residual, abs(incident_count - 2))
            incident_count == 2 || throw(
                ROCellComplex3DClosureError(
                    "cell $(cell.id) ridge $(ridge_id) does not have a two-facet link"))
        end
        length(cell.vertex_ids) - length(cell.ridge_ids) +
            length(cell.facet_ids) == 2 || throw(ROCellComplex3DClosureError(
            "cell $(cell.id) boundary Euler/link closure is not two"))

        # At each cell vertex, intersect the boundary with a small sphere.  The
        # resulting link must be one cycle: every incident facet joins exactly
        # two incident ridges, every ridge has degree two, and the link is
        # connected.  This rules out pinched/nonmanifold closures that can still
        # satisfy the global Euler count.
        for vertex_id in cell.vertex_ids
            _ro3_checkpoint(cancel_check)
            link_ridges = [ridge_id for ridge_id in cell.ridge_ids if
                vertex_id in ridges[ridge_id].vertex_ids]
            link_facets = [facet_id for facet_id in cell.facet_ids if
                vertex_id in facets[facet_id].vertex_ids]
            adjacency = Dict(ridge_id => Set{Int}() for ridge_id in link_ridges)
            for facet_id in link_facets
                _ro3_checkpoint(cancel_check)
                facet_link_ridges = [ridge_id for ridge_id in
                    facets[facet_id].ridge_ids if
                    vertex_id in ridges[ridge_id].vertex_ids]
                length(facet_link_ridges) == 2 || throw(
                    ROCellComplex3DClosureError(
                        "cell $(cell.id) vertex $(vertex_id) facet $(facet_id) " *
                        "does not meet exactly two link ridges"))
                first_ridge, second_ridge = facet_link_ridges
                push!(adjacency[first_ridge], second_ridge)
                push!(adjacency[second_ridge], first_ridge)
            end
            degree_residual = maximum((abs(length(neighbors) - 2)
                for neighbors in values(adjacency)); init=0)
            maximum_vertex_link_degree_residual = max(
                maximum_vertex_link_degree_residual, degree_residual)
            degree_residual == 0 ||
                throw(ROCellComplex3DClosureError(
                    "cell $(cell.id) vertex $(vertex_id) link is not degree two"))
            visited = Set{Int}()
            pending = isempty(link_ridges) ? Int[] : [first(link_ridges)]
            while !isempty(pending)
                _ro3_checkpoint(cancel_check)
                ridge_id = pop!(pending)
                ridge_id in visited && continue
                push!(visited, ridge_id)
                append!(pending, collect(adjacency[ridge_id]))
            end
            if length(visited) != length(link_ridges)
                disconnected_vertex_link_count += 1
                throw(ROCellComplex3DClosureError(
                    "cell $(cell.id) vertex $(vertex_id) link is disconnected"))
            end
        end
    end

    euler_value = length(vertex_objects) - length(ridges) + length(facets) -
        length(cell_objects)
    euler_consistent = euler_value == 1
    euler_consistent || throw(ROCellComplex3DClosureError(
        "global Euler value is $(euler_value), expected 1"))
    has_ambiguity = any(cell -> cell.set_valued, cell_objects) ||
        any(facet -> facet.ambiguous, facets) ||
        any(stratum -> stratum.kind === :ambiguous, strata)
    has_singular = any(stratum -> stratum.kind === :singular, strata)
    exact_no_overlap = exact_certificate.pair_dimension_certified &&
        exact_certificate.dimension_counts[5] == 0
    exact_no_overlap || throw(ROCellComplex3DClosureError(
        "exact pair-dimension certificate did not establish non-overlap"))
    maximum_pair_overlap_unit_volume = exact_no_overlap ? 0.0 : Inf
    publishable = !has_ambiguity && cell_facet_validation.valid &&
        facet_ridge_closure && ridge_vertex_links &&
        all(domain_side_coverage) && euler_consistent &&
        exact_certificate.pair_dimension_certified &&
        exact_certificate.support_coverage_certified &&
        exact_certificate.volume_coverage_certified && exact_no_overlap &&
        maximum_ridge_cell_link_residual == 0 &&
        maximum_facet_boundary_residual == 0 &&
        maximum_vertex_link_degree_residual == 0 &&
        disconnected_vertex_link_count == 0
    certificate = ROCellComplex3DCertificate(_RO3_VALIDATED,
        true, exact_no_overlap, cell_facet_validation.valid,
        facet_ridge_closure, ridge_vertex_links, domain_side_coverage,
        exact_certificate.volume_coverage_certified,
        euler_value, euler_consistent, publishable,
        exact_certificate.pair_dimension_certified,
        exact_certificate.support_coverage_certified,
        exact_certificate.volume_coverage_certified,
        exact_certificate.dimension_counts,
        exact_certificate.exact_volume_sum,
        exact_certificate.exact_domain_volume,
        maximum_pair_overlap_unit_volume,
        cell_facet_validation.maximum_area_residual,
        cell_facet_validation.maximum_overlap_area,
        domain_validation.maximum_area_residual,
        domain_validation.maximum_overlap_area,
        unit_volume_sum, unit_volume_residual,
        maximum_ridge_cell_link_residual,
        maximum_facet_boundary_residual,
        maximum_vertex_link_degree_residual,
        disconnected_vertex_link_count,
        physical.publication.length,
        physical.publication.area,
        physical.publication.volume,
        RO_CELL_COMPLEX_3D_EVIDENCE_SCOPE,
        false, false, false)
    payload = _ro3_canonical_payload(domain, specs, annotations,
        cell_objects, facets, ridges, vertex_objects, strata, certificate,
        has_singular, has_ambiguity, tolerances, limits, cancel_check,
        BigInt(preconstruction_work) + ridge_scan_work)
    identity = _ro3_payload_identity(payload)
    result = ROCellComplex3D(_RO3_VALIDATED,
        RO_CELL_COMPLEX_3D_VERSION, domain, cell_objects,
        facets, ridges, vertex_objects, strata, certificate, has_singular,
        has_ambiguity, specs, annotations, limits, payload, identity,
        tolerances, RO_CELL_COMPLEX_3D_EVIDENCE_SCOPE)
    validate_result && _ro3_validate_ro_cell_complex_3d(
        result, cancel_check, true)
    return result
end

function _ro3_validation_failure(message)
    throw(ROCellComplex3DClosureError(
        "stored 3D complex validation failed: $(message)"))
end

"""
    validate_ro_cell_complex_3d(complex; cancel_check=() -> nothing)

Recompute the content payload, SHA-256 root, independent publication decision,
and the complete D=3 Float64-input construction, including its exact dyadic
pair/support/volume decisions, from the retained normalized inputs. Any stale
mutable field, forged output/certificate, or loosened publication claim fails
closed. This validation does not upgrade the evidence to arbitrary-real
precision, D >= 4, or chemistry extraction.
"""
function validate_ro_cell_complex_3d(complex::ROCellComplex3D;
    cancel_check=() -> nothing)
    return _ro3_validate_ro_cell_complex_3d(complex, cancel_check, true)
end

function _ro3_validate_ro_cell_complex_3d(complex::ROCellComplex3D,
    cancel_check, rebuild::Bool)
    _ro3_checkpoint(cancel_check)
    complex.schema_version == RO_CELL_COMPLEX_3D_VERSION ||
        _ro3_validation_failure("schema version mismatch")
    complex.evidence_scope == RO_CELL_COMPLEX_3D_EVIDENCE_SCOPE ||
        _ro3_validation_failure("evidence scope mismatch")
    certificate = complex.certificate
    certificate.evidence_scope == RO_CELL_COMPLEX_3D_EVIDENCE_SCOPE ||
        _ro3_validation_failure("certificate evidence scope mismatch")
    certificate.arbitrary_precision_certified &&
        _ro3_validation_failure("arbitrary-precision evidence was overclaimed")
    certificate.higher_dimension_certified &&
        _ro3_validation_failure("higher-dimensional evidence was overclaimed")
    certificate.chemistry_extraction_certified &&
        _ro3_validation_failure("chemistry-extraction evidence was overclaimed")
    quantitative = Float64[
        certificate.maximum_pair_overlap_unit_volume,
        certificate.maximum_cell_facet_area_residual,
        certificate.maximum_cell_facet_overlap_area,
        certificate.maximum_domain_side_area_residual,
        certificate.maximum_domain_side_overlap_area,
        certificate.unit_volume_sum,
        certificate.unit_volume_residual,
        certificate.publication_length_tolerance,
        certificate.publication_area_tolerance,
        certificate.publication_volume_tolerance,
    ]
    all(isfinite, quantitative) ||
        _ro3_validation_failure("certificate contains a non-finite quantity")
    all(>=(0), quantitative) ||
        _ro3_validation_failure("certificate contains a negative residual")
    certificate.exact_pair_dimension_certified ||
        _ro3_validation_failure("exact dyadic pair dimensions are uncertified")
    certificate.exact_support_coverage_certified ||
        _ro3_validation_failure("exact dyadic support coverage is uncertified")
    certificate.exact_volume_coverage_certified ||
        _ro3_validation_failure("exact dyadic volume coverage is uncertified")
    all(>=(0), certificate.exact_pair_dimension_counts) ||
        _ro3_validation_failure("exact pair-dimension counts are negative")
    sum(certificate.exact_pair_dimension_counts) ==
        length(complex.cells) * (length(complex.cells) - 1) ÷ 2 ||
        _ro3_validation_failure("exact pair-dimension population is incomplete")
    certificate.exact_pair_dimension_counts[5] == 0 ||
        _ro3_validation_failure("exact positive-volume pair overlap is present")
    certificate.maximum_pair_overlap_unit_volume == 0.0 ||
        _ro3_validation_failure("exact non-overlap metric is not zero")
    certificate.exact_cell_volume_sum == certificate.exact_domain_volume ||
        _ro3_validation_failure("exact cell and domain volumes differ")
    certificate.euler_consistent == (certificate.euler_value == 1) ||
        _ro3_validation_failure("Euler value/flag are inconsistent")
    certificate.volume_complete ==
        certificate.exact_volume_coverage_certified ||
        _ro3_validation_failure("exact volume certificate/flag are inconsistent")
    abs(abs(certificate.unit_volume_sum - 1.0) -
        certificate.unit_volume_residual) <= 8eps(Float64) ||
        _ro3_validation_failure("volume sum/residual are inconsistent")

    derived_publishable = !complex.has_ambiguity &&
        certificate.face_dimension_agreement &&
        certificate.no_positive_volume_overlap &&
        certificate.exact_pair_dimension_certified &&
        certificate.exact_support_coverage_certified &&
        certificate.exact_volume_coverage_certified &&
        certificate.exact_pair_dimension_counts[5] == 0 &&
        certificate.exact_cell_volume_sum == certificate.exact_domain_volume &&
        certificate.cell_facet_closure && certificate.facet_ridge_closure &&
        certificate.ridge_vertex_links && all(certificate.domain_side_coverage) &&
        certificate.volume_complete && certificate.euler_consistent &&
        certificate.maximum_pair_overlap_unit_volume <=
            certificate.publication_volume_tolerance &&
        certificate.maximum_cell_facet_area_residual <=
            certificate.publication_area_tolerance &&
        certificate.maximum_cell_facet_overlap_area <=
            certificate.publication_area_tolerance &&
        certificate.maximum_domain_side_area_residual <=
            certificate.publication_area_tolerance &&
        certificate.maximum_domain_side_overlap_area <=
            certificate.publication_area_tolerance &&
        certificate.maximum_ridge_cell_link_residual == 0 &&
        certificate.maximum_facet_boundary_residual == 0 &&
        certificate.maximum_vertex_link_degree_residual == 0 &&
        certificate.disconnected_vertex_link_count == 0
    certificate.publishable == derived_publishable ||
        _ro3_validation_failure("stored publishable flag is not derived")

    payload = _ro3_canonical_payload(complex.domain, complex.source_specs,
        complex.interface_annotations, complex.cells, complex.facets,
        complex.ridges, complex.vertices, complex.strata, certificate,
        complex.has_singular_strata, complex.has_ambiguity,
        complex.tolerances, complex.construction_limits, cancel_check)
    payload == complex.canonical_payload ||
        _ro3_validation_failure("canonical content payload is stale")
    identity = _ro3_payload_identity(payload)
    identity == complex.canonical_identity ||
        _ro3_validation_failure("SHA-256 content root is stale")

    if rebuild
        expected = _ro3_build_ro_cell_complex_3d(complex.domain,
            complex.source_specs, complex.interface_annotations,
            complex.tolerances, complex.construction_limits,
            cancel_check, false)
        expected.canonical_payload == payload ||
            _ro3_validation_failure(
                "stored scientific/geometric content differs from a full rebuild")
        expected.canonical_identity == identity ||
            _ro3_validation_failure("full rebuild SHA-256 root differs")
    end
    return true
end
