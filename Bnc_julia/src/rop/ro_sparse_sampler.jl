using LinearAlgebra

# Deterministic, demonstration-scale dimension-adaptive sparse sampling.
# This substrate produces finite-policy evidence only.  It deliberately makes
# no continuum approximation-error, convergence-rate, or robustness claim.

const RO_SPARSE_POLICY_VERSION =
    "bne-ro-sparse-smolyak/v1.0.0"
const RO_SPARSE_EVIDENCE_SCOPE = :finite_adaptive_policy_only
const _ROSS_HARD_MAX_DIMENSIONS = 6
const _ROSS_HARD_MAX_OUTPUTS = 64
const _ROSS_HARD_MAX_LEVEL = 12
const _ROSS_HARD_MAX_POINTS = 1_000_000
const _ROSS_HARD_MAX_MULTI_INDICES = 65_536
const _ROSS_HARD_MAX_WORK = 1_000_000_000
const _ROSS_HARD_MAX_PAYLOAD_SCALARS = 64_000_000
const _ROSS_V2_MAX_PORTABLE_TOKEN_BYTES = 64 * 1024 * 1024
const _ROSS_V2_MAX_PORTABLE_TOKEN_DEPTH = 64

"Raised before a sparse-sampling candidate exceeds a declared hard budget."
struct ROSparseSamplingLimitExceeded <: Exception
    phase::Symbol
    requested::BigInt
    limit::Int
end

# ---------------------------------------------------------------------------

function Base.showerror(io::IO, err::ROSparseSamplingLimitExceeded)
    print(io, "adaptive RO sparse sampling ", err.phase, " requires ",
        err.requested, ", exceeding limit=", err.limit)
end

struct ROSparseSamplingLimits
    max_level::Int
    max_points::Int
    max_multi_indices::Int
    max_work::Int
    max_payload_scalars::Int

    function ROSparseSamplingLimits(
        max_level::Integer,
        max_points::Integer,
        max_multi_indices::Integer,
        max_work::Integer,
        max_payload_scalars::Integer,
    )
        return new(
            _ross_positive_bounded(
                max_level, "max_level", _ROSS_HARD_MAX_LEVEL),
            _ross_positive_bounded(
                max_points, "max_points", _ROSS_HARD_MAX_POINTS),
            _ross_positive_bounded(
                max_multi_indices, "max_multi_indices",
                _ROSS_HARD_MAX_MULTI_INDICES),
            _ross_positive_bounded(
                max_work, "max_work", _ROSS_HARD_MAX_WORK),
            _ross_positive_bounded(
                max_payload_scalars, "max_payload_scalars",
                _ROSS_HARD_MAX_PAYLOAD_SCALARS),
        )
    end
end

function _ross_positive_bounded(raw, name::AbstractString, hard_max::Int)
    (raw isa Integer && !(raw isa Bool)) || throw(ArgumentError(
        "$name must be an integer"))
    value = try
        Int(raw)
    catch
        throw(ArgumentError("$name must fit in Int"))
    end
    0 < value <= hard_max || throw(ArgumentError(
        "$name must lie in 1:$hard_max"))
    return value
end

function ROSparseSamplingLimits(;
    max_level::Integer=6,
    max_points::Integer=4_096,
    max_multi_indices::Integer=256,
    max_work::Integer=10_000_000,
    max_payload_scalars::Integer=262_144,
)
    return ROSparseSamplingLimits(
        _ross_positive_bounded(max_level, "max_level", _ROSS_HARD_MAX_LEVEL),
        _ross_positive_bounded(max_points, "max_points", _ROSS_HARD_MAX_POINTS),
        _ross_positive_bounded(
            max_multi_indices, "max_multi_indices",
            _ROSS_HARD_MAX_MULTI_INDICES),
        _ross_positive_bounded(max_work, "max_work", _ROSS_HARD_MAX_WORK),
        _ross_positive_bounded(
            max_payload_scalars, "max_payload_scalars",
            _ROSS_HARD_MAX_PAYLOAD_SCALARS),
    )
end

function _ross_normalize_limits(limits::ROSparseSamplingLimits)
    return ROSparseSamplingLimits(
        max_level=limits.max_level,
        max_points=limits.max_points,
        max_multi_indices=limits.max_multi_indices,
        max_work=limits.max_work,
        max_payload_scalars=limits.max_payload_scalars,
    )
end

@inline function _ross_limit(phase::Symbol, requested::BigInt, limit::Int)
    requested <= limit || throw(ROSparseSamplingLimitExceeded(
        phase, requested, limit))
    return nothing
end

"A detached plan binding chart, domain, outputs, and the full background."
struct ROSparseSamplingPlan
    schema_version::String
    chart_id::String
    control_ids::Vector{String}
    source_coordinate_ids::Vector{String}
    chart_jacobian::Matrix{Float64}
    domain_lower::Vector{Float64}
    domain_upper::Vector{Float64}
    output_ids::Vector{String}
    fixed_background::Vector{Float64}
    surplus_tolerance::Float64
    initial_total_degree::Int
end

function _ross_ids(raw, name::AbstractString, maximum::Int)
    (raw isa AbstractVector || raw isa Tuple) || throw(ArgumentError(
        "$name must be an ordered collection"))
    1 <= length(raw) <= maximum || throw(ArgumentError(
        "$name must contain 1:$maximum identifiers"))
    values = String[]
    for value in raw
        (value isa AbstractString || value isa Symbol) || throw(ArgumentError(
            "$name identifiers must be strings or symbols"))
        text = String(value)
        isempty(text) && throw(ArgumentError(
            "$name identifiers must not be empty"))
        ncodeunits(text) <= 128 || throw(ArgumentError(
            "$name identifiers must not exceed 128 UTF-8 bytes"))
        push!(values, text)
    end
    allunique(values) || throw(ArgumentError(
        "$name identifiers must be unique and ordered"))
    return values
end

function _ross_finite_vector(raw, name::AbstractString)
    raw isa AbstractVector || throw(ArgumentError("$name must be a vector"))
    all(value -> value isa Real && !(value isa Bool), raw) ||
        throw(ArgumentError("$name must contain real non-Boolean values"))
    values = Float64.(raw)
    all(isfinite, values) || throw(ArgumentError(
        "$name must contain only finite values"))
    # JSON has one mathematical zero.  Normalizing signed zero here keeps the
    # in-memory plan and its portable SHA-256 identity in one-to-one
    # correspondence instead of allowing `-0.0` and `0.0` to name the same
    # coordinates with different JSON3 byte spellings.
    for index in eachindex(values)
        values[index] == 0.0 && (values[index] = 0.0)
    end
    return values
end

function ROSparseSamplingPlan(;
    chart_id,
    control_ids,
    source_coordinate_ids,
    chart_jacobian,
    domain_lower,
    domain_upper,
    output_ids,
    fixed_background,
    surplus_tolerance::Real=1e-8,
    initial_total_degree::Integer=2,
)
    (chart_id isa AbstractString || chart_id isa Symbol) ||
        throw(ArgumentError("chart_id must be a string or symbol"))
    chart_name = String(chart_id)
    isempty(chart_name) && throw(ArgumentError("chart_id must not be empty"))
    controls = _ross_ids(
        control_ids, "control_ids", _ROSS_HARD_MAX_DIMENSIONS)
    sources = _ross_ids(
        source_coordinate_ids, "source_coordinate_ids", 1_024)
    outputs = _ross_ids(output_ids, "output_ids", _ROSS_HARD_MAX_OUTPUTS)
    lower = _ross_finite_vector(domain_lower, "domain_lower")
    upper = _ross_finite_vector(domain_upper, "domain_upper")
    background = _ross_finite_vector(
        fixed_background, "fixed_background")
    dimension = length(controls)
    length(lower) == length(upper) == dimension || throw(DimensionMismatch(
        "domain bounds must match the ordered control dimension"))
    all(lower .< upper) || throw(ArgumentError(
        "every sparse-sampling domain span must be positive"))
    length(background) == length(sources) || throw(DimensionMismatch(
        "fixed_background must match source_coordinate_ids"))

    chart_jacobian isa AbstractMatrix || throw(ArgumentError(
        "chart_jacobian must be a matrix"))
    all(value -> value isa Real && !(value isa Bool), chart_jacobian) ||
        throw(ArgumentError(
            "chart_jacobian must contain real non-Boolean values"))
    jacobian = Matrix{Float64}(chart_jacobian)
    all(isfinite, jacobian) || throw(ArgumentError(
        "chart_jacobian must contain only finite values"))
    for index in eachindex(jacobian)
        jacobian[index] == 0.0 && (jacobian[index] = 0.0)
    end
    size(jacobian) == (length(sources), dimension) ||
        throw(DimensionMismatch(
            "chart_jacobian must have source_count x control_count shape"))
    length(sources) >= dimension || throw(DimensionMismatch(
        "a full-column-rank chart requires source_count >= control_count"))
    singular_values = svdvals(jacobian)
    largest = first(singular_values)
    threshold = 1e-12 * largest
    count(value -> value > threshold, singular_values) == dimension ||
        throw(ArgumentError(
            "chart_jacobian must have full numerical column rank"))

    tolerance = Float64(surplus_tolerance)
    isfinite(tolerance) && tolerance > 0.0 || throw(ArgumentError(
        "surplus_tolerance must be finite and positive"))
    initial_degree = try
        Int(initial_total_degree)
    catch
        throw(ArgumentError("initial_total_degree must fit in Int"))
    end
    initial_degree == 2 || throw(ArgumentError(
        "the v1 adaptive policy requires initial_total_degree=2 so pure " *
        "multi-input interactions are probed before the stopping rule"))
    return ROSparseSamplingPlan(
        RO_SPARSE_POLICY_VERSION,
        chart_name,
        copy(controls),
        copy(sources),
        copy(jacobian),
        copy(lower),
        copy(upper),
        copy(outputs),
        copy(background),
        tolerance,
        initial_degree,
    )
end

function _ross_normalize_plan(plan::ROSparseSamplingPlan)
    plan.schema_version == RO_SPARSE_POLICY_VERSION || throw(ArgumentError(
        "unsupported sparse-sampling plan version"))
    return ROSparseSamplingPlan(
        chart_id=plan.chart_id,
        control_ids=plan.control_ids,
        source_coordinate_ids=plan.source_coordinate_ids,
        chart_jacobian=plan.chart_jacobian,
        domain_lower=plan.domain_lower,
        domain_upper=plan.domain_upper,
        output_ids=plan.output_ids,
        fixed_background=plan.fixed_background,
        surplus_tolerance=plan.surplus_tolerance,
        initial_total_degree=plan.initial_total_degree,
    )
end

"Portable scientific/policy identity; execution budgets are deliberately separate."
function ro_sparse_sampling_spec(raw_plan::ROSparseSamplingPlan)
    plan = _ross_normalize_plan(raw_plan)
    jacobian_rows = [
        Float64[plan.chart_jacobian[row, column]
            for column in axes(plan.chart_jacobian, 2)]
        for row in axes(plan.chart_jacobian, 1)
    ]
    return (
        schema_version=plan.schema_version,
        chart=(
            chart_id=plan.chart_id,
            control_ids=copy(plan.control_ids),
            source_coordinate_ids=copy(plan.source_coordinate_ids),
            jacobian=jacobian_rows,
        ),
        domain=(
            lower=copy(plan.domain_lower),
            upper=copy(plan.domain_upper),
        ),
        outputs=(output_ids=copy(plan.output_ids),),
        fixed_background=copy(plan.fixed_background),
        policy=(
            initial_total_degree=plan.initial_total_degree,
            indicator="full_output_linf_hierarchical_surplus",
            frontier_order="indicator_desc_total_level_asc_lexicographic",
            invalid_policy="unresolved_cone_blocks_descendants_only",
            surplus_tolerance=plan.surplus_tolerance,
        ),
    )
end

function _ross_sha256(value)
    return bytes2hex(SHA.sha256(codeunits(JSON3.write(value))))
end

function _ross_v2_assert_portable_token_size(
    payload,
    name,
    cancel_check=() -> nothing,
)
    cancel_check()
    byte_count = ncodeunits(JSON3.write(payload))
    cancel_check()
    byte_count <= _ROSS_V2_MAX_PORTABLE_TOKEN_BYTES ||
        throw(ROSparseSamplingLimitExceeded(
            Symbol(name * "_portable_token_bytes"), BigInt(byte_count),
            _ROSS_V2_MAX_PORTABLE_TOKEN_BYTES))
    return byte_count
end

ro_sparse_sampling_spec_sha256(plan::ROSparseSamplingPlan) =
    _ross_sha256(ro_sparse_sampling_spec(plan))

function ro_sparse_adaptive_plan_sha256(
    plan::ROSparseSamplingPlan,
    limits::ROSparseSamplingLimits=ROSparseSamplingLimits(),
)
    limits = _ross_normalize_limits(limits)
    identity = (
        sampling_spec_sha256=ro_sparse_sampling_spec_sha256(plan),
        execution_budget=(
            max_level=limits.max_level,
            max_points=limits.max_points,
            max_multi_indices=limits.max_multi_indices,
            max_work=limits.max_work,
            max_payload_scalars=limits.max_payload_scalars,
        ),
    )
    return _ross_sha256(identity)
end

"Strict callback result: valid finite values, or an explicit invalid reason."
struct ROSparseEvaluation
    valid::Bool
    values::Union{Nothing,Vector{Float64}}
    invalid_reason::Union{Nothing,Symbol}
end

function ro_sparse_valid(values::AbstractVector)
    all(value -> value isa Real && !(value isa Bool), values) ||
        throw(ArgumentError("valid sparse evaluations must be real-valued"))
    normalized = Float64.(values)
    all(isfinite, normalized) || throw(ArgumentError(
        "valid sparse evaluations must be finite"))
    return ROSparseEvaluation(true, normalized, nothing)
end

function ro_sparse_invalid(reason::Symbol)
    reason in (:none, :valid) && throw(ArgumentError(
        "invalid sparse evaluations require a meaningful reason"))
    return ROSparseEvaluation(false, nothing, reason)
end

struct ROSparsePointRequest
    point_id::String
    multi_index::Vector{Int}
    node_ids::Vector{String}
    normalized_coordinates::Vector{Float64}
    control_coordinates::Vector{Float64}
    source_coordinates::Vector{Float64}
end

struct ROSparseSample
    request::ROSparsePointRequest
    evaluation::ROSparseEvaluation
    surplus::Union{Nothing,Vector{Float64}}
end

struct ROSparseIndexRecord
    multi_index::Vector{Int}
    point_ids::Vector{String}
    status::Symbol
    indicator::Union{Nothing,Float64}
end

struct ROSparseUnresolvedRegion
    multi_index::Vector{Int}
    point_ids::Vector{String}
    reasons::Vector{Symbol}
end

"""
Portable, detached receipt for one multi-index after it has been committed.

The receipt is deliberately callback-safe: it contains only immutable JSON-like
values and therefore cannot be used to mutate the sampler's in-memory state.
It is suitable for content-addressed checkpoint/chunk layers, but by itself it
does not prove that an external worker evaluated the point or that the finite
adaptive policy has reached a terminal stopping condition.
"""
function ro_sparse_index_commit_payload(
    record::ROSparseIndexRecord,
    samples::AbstractVector{<:ROSparseSample},
)
    Tuple(record.point_ids) == Tuple(
        sample.request.point_id for sample in samples) || throw(ArgumentError(
        "index commit samples must exactly match the record point order"))
    Tuple(record.multi_index) == Tuple(first(samples).request.multi_index) ||
        throw(ArgumentError(
            "index commit samples must match the record multi-index"))
    all(sample -> Tuple(sample.request.multi_index) ==
            Tuple(record.multi_index), samples) || throw(ArgumentError(
        "one index commit cannot mix samples from different multi-indices"))
    return (
        multi_index=Tuple(record.multi_index),
        status=String(record.status),
        indicator=record.indicator,
        point_ids=Tuple(record.point_ids),
        samples=Tuple((
            point_id=sample.request.point_id,
            node_ids=Tuple(sample.request.node_ids),
            normalized_coordinates=Tuple(
                sample.request.normalized_coordinates),
            control_coordinates=Tuple(sample.request.control_coordinates),
            source_coordinates=Tuple(sample.request.source_coordinates),
            valid=sample.evaluation.valid,
            values=sample.evaluation.values === nothing ? nothing :
                Tuple(sample.evaluation.values),
            invalid_reason=sample.evaluation.invalid_reason === nothing ?
                nothing : String(sample.evaluation.invalid_reason),
            surplus=sample.surplus === nothing ? nothing :
                Tuple(sample.surplus),
        ) for sample in samples),
    )
end

"Finite deterministic-policy result; `continuum_error_bound` is always null."
struct ROSparseSamplingResult
    plan::ROSparseSamplingPlan
    limits::ROSparseSamplingLimits
    sampling_spec_sha256::String
    adaptive_plan_sha256::String
    status::Symbol
    stopping_reason::Symbol
    evidence_scope::Symbol
    continuum_error_bound::Nothing
    accepted_multi_indices::Vector{Vector{Int}}
    active_frontier::Vector{Vector{Int}}
    refinement_order::Vector{Vector{Int}}
    index_records::Vector{ROSparseIndexRecord}
    samples::Vector{ROSparseSample}
    unresolved_regions::Vector{ROSparseUnresolvedRegion}
    evaluated_point_count::Int
    valid_point_count::Int
    invalid_point_count::Int
    work_units_consumed::Int
    payload_scalar_count::Int
    max_active_indicator::Union{Nothing,Float64}
end

struct _ROSparseCCNode
    introduction_level::Int
    numerator::Int
    denominator::Int
    node_id::String
    coordinate::Float64
end

function _ross_increment_count(level::Int)
    level == 1 && return BigInt(1)
    level == 2 && return BigInt(2)
    return BigInt(1) << (level - 2)
end

function _ross_full_count(level::Int)
    level == 1 && return BigInt(1)
    return (BigInt(1) << (level - 1)) + 1
end

function _ross_increment_nodes(level::Int)
    1 <= level <= _ROSS_HARD_MAX_LEVEL || throw(ArgumentError(
        "Clenshaw-Curtis level must lie in 1:$(_ROSS_HARD_MAX_LEVEL)"))
    if level == 1
        return [_ROSparseCCNode(1, 1, 2, "cc:1/2", 0.0)]
    elseif level == 2
        return [
            _ROSparseCCNode(2, 0, 1, "cc:0/1", -1.0),
            _ROSparseCCNode(2, 1, 1, "cc:1/1", 1.0),
        ]
    end
    denominator = 1 << (level - 1)
    nodes = _ROSparseCCNode[]
    for numerator in 1:2:(denominator - 1)
        coordinate = -cospi(numerator / denominator)
        push!(nodes, _ROSparseCCNode(
            level,
            numerator,
            denominator,
            "cc:$(numerator)/$(denominator)",
            coordinate == 0.0 ? 0.0 : coordinate,
        ))
    end
    return nodes
end

function _ross_full_nodes(level::Int)
    nodes = _ROSparseCCNode[]
    for current_level in 1:level
        append!(nodes, _ross_increment_nodes(current_level))
    end
    sort!(nodes; by=node -> (node.coordinate, node.node_id))
    return nodes
end

"Nested Clenshaw-Curtis-Lobatto coordinates on the canonical interval [-1,1]."
function nested_clenshaw_curtis_nodes(level::Integer)
    value = _ross_positive_bounded(
        level, "level", _ROSS_HARD_MAX_LEVEL)
    return Float64[node.coordinate for node in _ross_full_nodes(value)]
end

"New one-dimensional nodes introduced at exactly `level`."
function incremental_clenshaw_curtis_nodes(level::Integer)
    value = _ross_positive_bounded(
        level, "level", _ROSS_HARD_MAX_LEVEL)
    return Float64[node.coordinate for node in _ross_increment_nodes(value)]
end

function _ross_normalize_multi_indices(raw_indices)
    (raw_indices isa AbstractVector || raw_indices isa Tuple) ||
        throw(ArgumentError("multi_indices must be an ordered collection"))
    isempty(raw_indices) && throw(ArgumentError(
        "multi_indices must not be empty"))
    first_raw = first(raw_indices)
    (first_raw isa AbstractVector || first_raw isa Tuple) ||
        throw(ArgumentError("each Smolyak multi-index must be a vector"))
    dimension = length(first_raw)
    1 <= dimension <= _ROSS_HARD_MAX_DIMENSIONS || throw(ArgumentError(
        "Smolyak multi-index dimension is unsupported"))
    indices = Tuple[]
    for raw in raw_indices
        (raw isa AbstractVector || raw isa Tuple) || throw(ArgumentError(
            "each Smolyak multi-index must be a vector"))
        length(raw) == dimension || throw(DimensionMismatch(
            "all Smolyak multi-indices must have one dimension"))
        values = Int[]
        for value in raw
            (value isa Integer && !(value isa Bool)) || throw(ArgumentError(
                "Smolyak levels must be positive integers"))
            normalized = try
                Int(value)
            catch
                throw(ArgumentError("Smolyak levels must fit in Int"))
            end
            normalized >= 1 || throw(ArgumentError(
                "Smolyak levels must be positive"))
            push!(values, normalized)
        end
        push!(indices, Tuple(values))
    end
    allunique(indices) || throw(ArgumentError(
        "Smolyak multi-indices must be unique"))
    return indices, dimension
end

function smolyak_is_downward_closed(raw_indices)
    indices, dimension = _ross_normalize_multi_indices(raw_indices)
    index_set = Set(indices)
    for index in indices, axis in 1:dimension
        index[axis] == 1 && continue
        predecessor = ntuple(position ->
            position == axis ? index[position] - 1 : index[position],
            dimension)
        predecessor in index_set || return false
    end
    return true
end

function _ross_is_admissible(candidate::Tuple, accepted::Set{Tuple})
    dimension = length(candidate)
    for axis in 1:dimension
        candidate[axis] == 1 && continue
        predecessor = ntuple(position ->
            position == axis ? candidate[position] - 1 : candidate[position],
            dimension)
        predecessor in accepted || return false
    end
    return true
end

"Deterministic lexicographic admissible frontier of a downward-closed set."
function smolyak_admissible_frontier(raw_indices; max_level::Integer=12)
    indices, dimension = _ross_normalize_multi_indices(raw_indices)
    smolyak_is_downward_closed(raw_indices) || throw(ArgumentError(
        "Smolyak multi_indices must be downward closed"))
    level_limit = _ross_positive_bounded(
        max_level, "max_level", _ROSS_HARD_MAX_LEVEL)
    accepted = Set(indices)
    frontier = Set{Tuple}()
    for index in indices, axis in 1:dimension
        index[axis] >= level_limit && continue
        candidate = ntuple(position ->
            position == axis ? index[position] + 1 : index[position],
            dimension)
        candidate in accepted && continue
        _ross_is_admissible(candidate, accepted) && push!(frontier, candidate)
    end
    ordered = sort!(collect(frontier))
    return [collect(index) for index in ordered]
end

function _ross_each_product(callback, vectors)
    dimension = length(vectors)
    current = Vector{Any}(undef, dimension)
    function visit(axis)
        if axis > dimension
            callback(copy(current))
            return
        end
        for value in vectors[axis]
            current[axis] = value
            visit(axis + 1)
        end
    end
    visit(1)
    return nothing
end

_ross_point_id(spec_sha256::AbstractString, nodes) =
    "ross:" * String(spec_sha256) * ":cc-point[" *
    join((node.node_id for node in nodes), "|") * "]"

function _ross_point_request(plan::ROSparseSamplingPlan,
                             spec_sha256::AbstractString,
                             multi_index::Tuple, nodes)
    normalized = Float64[node.coordinate for node in nodes]
    control = plan.domain_lower .+
        ((normalized .+ 1.0) ./ 2.0) .* (plan.domain_upper .- plan.domain_lower)
    source = plan.fixed_background + plan.chart_jacobian * control
    all(isfinite, control) && all(isfinite, source) || throw(OverflowError(
        "sparse-sampling chart mapping produced non-finite coordinates"))
    return ROSparsePointRequest(
        _ross_point_id(spec_sha256, nodes),
        collect(multi_index),
        String[node.node_id for node in nodes],
        normalized,
        control,
        source,
    )
end

function _ross_lagrange_basis(nodes, coordinate::Float64)
    count = length(nodes)
    count == 1 && return [1.0]
    exact = findfirst(node -> node.coordinate == coordinate, nodes)
    if exact !== nothing
        basis = zeros(count)
        basis[exact] = 1.0
        return basis
    end
    terms = Vector{Float64}(undef, count)
    for index in 1:count
        endpoint_factor = index == 1 || index == count ? 0.5 : 1.0
        barycentric_weight = isodd(index - 1) ? -endpoint_factor : endpoint_factor
        terms[index] = barycentric_weight /
            (coordinate - nodes[index].coordinate)
    end
    denominator = sum(terms)
    isfinite(denominator) && denominator != 0.0 || throw(OverflowError(
        "Clenshaw-Curtis barycentric interpolation became non-finite"))
    basis = terms ./ denominator
    all(isfinite, basis) || throw(OverflowError(
        "Clenshaw-Curtis interpolation weights became non-finite"))
    return basis
end

function _ross_tensor_interpolate(
    levels::Tuple,
    target::Vector{Float64},
    evaluations::Dict{String,ROSparseEvaluation},
    output_count::Int,
    spec_sha256::AbstractString,
)
    axes = [_ross_full_nodes(level) for level in levels]
    bases = [_ross_lagrange_basis(axes[axis], target[axis])
        for axis in eachindex(axes)]
    weighted = zeros(output_count)
    complete = Ref(true)
    _ross_each_product(axes) do nodes
        coefficient = 1.0
        for axis in eachindex(nodes)
            node_index = findfirst(==(nodes[axis]), axes[axis])
            coefficient *= bases[axis][node_index]
        end
        coefficient == 0.0 && return
        point_id = _ross_point_id(spec_sha256, nodes)
        evaluation = get(evaluations, point_id, nothing)
        evaluation === nothing && error(
            "internal sparse-grid closure is missing point $point_id")
        if !evaluation.valid
            complete[] = false
            return
        end
        @inbounds for output in 1:output_count
            weighted[output] += coefficient * evaluation.values[output]
        end
    end
    complete[] || return nothing
    all(isfinite, weighted) || throw(OverflowError(
        "hierarchical sparse interpolation produced non-finite values"))
    return weighted
end

function _ross_mixed_surplus(
    multi_index::Tuple,
    request::ROSparsePointRequest,
    evaluations::Dict{String,ROSparseEvaluation},
    output_count::Int,
    spec_sha256::AbstractString,
)
    dimension = length(multi_index)
    surplus = zeros(output_count)
    for mask in 0:((1 << dimension) - 1)
        levels = ntuple(axis ->
            multi_index[axis] - ((mask >> (axis - 1)) & 1), dimension)
        any(level -> level < 1, levels) && continue
        term = _ross_tensor_interpolate(
            levels, request.normalized_coordinates,
            evaluations, output_count, spec_sha256)
        term === nothing && return nothing
        sign = isodd(count_ones(mask)) ? -1.0 : 1.0
        surplus .+= sign .* term
    end
    all(isfinite, surplus) || throw(OverflowError(
        "mixed hierarchical surplus became non-finite"))
    return surplus
end

function _ross_candidate_cost(index::Tuple, output_count::Int)
    point_count = prod(_ross_increment_count(level) for level in index;
        init=BigInt(1))
    dimension = length(index)
    interpolation_nodes = BigInt(0)
    for mask in 0:((1 << dimension) - 1)
        levels = ntuple(axis ->
            index[axis] - ((mask >> (axis - 1)) & 1), dimension)
        any(level -> level < 1, levels) && continue
        interpolation_nodes += prod(
            _ross_full_count(level) for level in levels;
            init=BigInt(1))
    end
    payload = point_count * BigInt(output_count)
    work = point_count + point_count * BigInt(output_count) *
        interpolation_nodes
    return point_count, payload, work
end

function _ross_budget_reason(
    index::Tuple,
    current_points::Int,
    current_indices::Int,
    current_work::Int,
    current_payload::Int,
    output_count::Int,
    limits::ROSparseSamplingLimits,
)
    maximum(index) <= limits.max_level || return :level_budget_exhausted
    point_count, payload, work = _ross_candidate_cost(index, output_count)
    BigInt(current_indices) + 1 <= limits.max_multi_indices ||
        return :multi_index_budget_exhausted
    BigInt(current_points) + point_count <= limits.max_points ||
        return :point_budget_exhausted
    BigInt(current_payload) + payload <= limits.max_payload_scalars ||
        return :payload_budget_exhausted
    BigInt(current_work) + work <= limits.max_work ||
        return :work_budget_exhausted
    return nothing
end

function _ross_initial_indices(dimension::Int, total_degree::Int)
    indices = Tuple[]
    current = ones(Int, dimension)
    function visit(axis::Int, remaining::Int)
        if axis > dimension
            push!(indices, Tuple(current))
            return
        end
        for increment in 0:remaining
            current[axis] = 1 + increment
            visit(axis + 1, remaining - increment)
        end
        current[axis] = 1
    end
    visit(1, total_degree)
    unique!(indices)
    sort!(indices; by=index -> (sum(index) - dimension, index))
    return indices
end

function _ross_in_blocked_cone(index::Tuple, blocked_cones)
    return any(cone -> all(index[axis] >= cone[axis]
        for axis in eachindex(index)), blocked_cones)
end

function _ross_active_indices(accepted, refined, records)
    resolved = Set(Tuple(record.multi_index) for record in records
        if record.indicator !== nothing)
    return sort!([index for index in accepted
        if !(index in refined) && index in resolved])
end

function _ross_result(
    plan,
    limits,
    status,
    stopping_reason,
    accepted_order,
    accepted,
    refined,
    refinement_order,
    records,
    samples,
    unresolved,
    work,
    payload,
)
    active = _ross_active_indices(accepted, refined, records)
    indicators = Float64[]
    records_by_index = Dict(Tuple(record.multi_index) => record for record in records)
    for index in active
        indicator = records_by_index[index].indicator
        indicator === nothing || push!(indicators, indicator)
    end
    valid_count = count(sample -> sample.evaluation.valid, samples)
    return ROSparseSamplingResult(
        plan,
        limits,
        ro_sparse_sampling_spec_sha256(plan),
        ro_sparse_adaptive_plan_sha256(plan, limits),
        status,
        stopping_reason,
        RO_SPARSE_EVIDENCE_SCOPE,
        nothing,
        [collect(index) for index in accepted_order],
        [collect(index) for index in active],
        [collect(index) for index in refinement_order],
        records,
        samples,
        unresolved,
        length(samples),
        valid_count,
        length(samples) - valid_count,
        work,
        payload,
        isempty(indicators) ? nothing : maximum(indicators),
    )
end

"""
    adaptive_sparse_ro_field(plan, evaluator; limits, cancel_check)

Run a deterministic dimension-adaptive Smolyak policy. Every index through
total degree two is a mandatory initial probe, so a pure pair interaction is
visible before any surplus stopping rule. Thereafter the active index with the
largest mixed hierarchical-surplus indicator is refined; exact ties use total
level and then lexicographic multi-index order. The returned positive status
describes only this evaluated finite policy. It is never a continuum error
bound.

`evaluator(request)` must return `ro_sparse_valid(values)` or
`ro_sparse_invalid(reason)`. One invalid point makes the whole associated
multi-index an unresolved region: neither that point nor its valid siblings
receive a numeric surplus, and zero is never substituted. Its descendant cone
is blocked, while incomparable valid branches may continue.
"""
function adaptive_sparse_ro_field(
    raw_plan::ROSparseSamplingPlan,
    evaluator;
    limits::ROSparseSamplingLimits=ROSparseSamplingLimits(),
    cancel_check=() -> nothing,
    index_commit_callback=(_ -> nothing),
)
    evaluator isa Function || throw(ArgumentError(
        "evaluator must be a callback function"))
    index_commit_callback isa Function || throw(ArgumentError(
        "index_commit_callback must be a callback function"))
    limits = _ross_normalize_limits(limits)
    plan = _ross_normalize_plan(raw_plan)
    sampling_spec_sha256 = ro_sparse_sampling_spec_sha256(plan)
    dimension = length(plan.control_ids)
    output_count = length(plan.output_ids)
    accepted = Set{Tuple}()
    accepted_order = Tuple[]
    refined = Set{Tuple}()
    refinement_order = Tuple[]
    records = ROSparseIndexRecord[]
    samples = ROSparseSample[]
    unresolved = ROSparseUnresolvedRegion[]
    evaluations = Dict{String,ROSparseEvaluation}()
    blocked_cones = Set{Tuple}()
    work_consumed = 0
    payload_consumed = 0

    function finish(status, reason)
        return _ross_result(
            plan, limits, status, reason, accepted_order, accepted,
            refined, refinement_order, records, samples, unresolved,
            work_consumed, payload_consumed)
    end

    function evaluate_index!(index::Tuple)
        reason = _ross_budget_reason(
            index, length(samples), length(accepted), work_consumed,
            payload_consumed, output_count, limits)
        reason === nothing || return reason
        point_count_big, payload_big, work_big =
            _ross_candidate_cost(index, output_count)
        point_count = Int(point_count_big)
        candidate_requests = ROSparsePointRequest[]
        candidate_evaluations = ROSparseEvaluation[]
        axes = [_ross_increment_nodes(level) for level in index]
        sizehint!(candidate_requests, point_count)
        sizehint!(candidate_evaluations, point_count)
        _ross_each_product(axes) do nodes
            request = _ross_point_request(
                plan, sampling_spec_sha256, index, nodes)
            haskey(evaluations, request.point_id) && error(
                "internal hierarchical point identity was evaluated twice")
            cancel_check()
            raw_evaluation = evaluator(request)
            # The callback may have observed cancellation while it ran.  No
            # candidate state is committed until this post-callback checkpoint
            # and the final pre-commit checkpoint below both succeed.
            cancel_check()
            raw_evaluation isa ROSparseEvaluation || throw(ArgumentError(
                "evaluator must return ROSparseEvaluation"))
            evaluation = raw_evaluation::ROSparseEvaluation
            if evaluation.valid
                evaluation.values !== nothing || error(
                    "valid evaluator result lost its values")
                length(evaluation.values) == output_count ||
                    throw(DimensionMismatch(
                        "evaluator output length must match output_ids"))
                all(isfinite, evaluation.values) || throw(ArgumentError(
                    "evaluator returned non-finite valid values"))
                evaluation.invalid_reason === nothing || throw(ArgumentError(
                    "valid evaluator result cannot carry an invalid reason"))
            else
                evaluation.values === nothing || throw(ArgumentError(
                    "invalid evaluator result cannot carry numeric values"))
                evaluation.invalid_reason === nothing && throw(ArgumentError(
                    "invalid evaluator result requires a reason"))
            end
            push!(candidate_requests, request)
            push!(candidate_evaluations, evaluation)
        end
        length(candidate_requests) == point_count || error(
            "internal sparse candidate point-count mismatch")
        cancel_check()

        for position in eachindex(candidate_requests)
            evaluations[candidate_requests[position].point_id] =
                candidate_evaluations[position]
        end
        push!(accepted, index)
        push!(accepted_order, index)
        work_consumed += Int(work_big)
        payload_consumed += Int(payload_big)

        invalid_positions = findall(evaluation -> !evaluation.valid,
            candidate_evaluations)
        point_ids = String[request.point_id for request in candidate_requests]
        if !isempty(invalid_positions)
            first_sample = length(samples) + 1
            for position in eachindex(candidate_requests)
                push!(samples, ROSparseSample(
                    candidate_requests[position],
                    candidate_evaluations[position],
                    nothing,
                ))
            end
            reasons = sort!(unique(Symbol[
                candidate_evaluations[position].invalid_reason
                for position in invalid_positions
            ]); by=string)
            record = ROSparseIndexRecord(
                collect(index), point_ids, :unresolved_gap, nothing)
            push!(records, record)
            push!(unresolved, ROSparseUnresolvedRegion(
                collect(index), point_ids, reasons))
            push!(blocked_cones, index)
            cancel_check()
            index_commit_callback(ro_sparse_index_commit_payload(
                record, @view(samples[first_sample:length(samples)])))
            cancel_check()
            return :unresolved_gap
        end

        candidate_surpluses = Vector{Vector{Float64}}()
        for request in candidate_requests
            cancel_check()
            surplus = _ross_mixed_surplus(
                index, request, evaluations, output_count,
                sampling_spec_sha256)
            surplus === nothing && error(
                "valid downward-closed candidate unexpectedly lacks a surplus")
            push!(candidate_surpluses, surplus)
        end
        indicator = maximum((maximum(abs, surplus)
            for surplus in candidate_surpluses); init=0.0)
        cancel_check()
        first_sample = length(samples) + 1
        for position in eachindex(candidate_requests)
            push!(samples, ROSparseSample(
                candidate_requests[position],
                candidate_evaluations[position],
                candidate_surpluses[position],
            ))
        end
        record = ROSparseIndexRecord(
            collect(index), point_ids, :resolved, indicator)
        push!(records, record)
        cancel_check()
        index_commit_callback(ro_sparse_index_commit_payload(
            record, @view(samples[first_sample:length(samples)])))
        cancel_check()
        return nothing
    end

    initial = _ross_initial_indices(dimension, plan.initial_total_degree)
    for candidate in initial
        _ross_in_blocked_cone(candidate, blocked_cones) && continue
        reason = evaluate_index!(candidate)
        reason === nothing && continue
        reason == :unresolved_gap && continue
        return finish(:partial_budget, reason)
    end
    # Initial indices below the outer total-degree shell have had every
    # admissible child in that shell evaluated (or explicitly blocked).
    records_by_index = Dict(
        Tuple(record.multi_index) => record for record in records)
    for index in initial
        index in accepted || continue
        sum(index) - dimension < plan.initial_total_degree || continue
        records_by_index[index].indicator === nothing && continue
        push!(refined, index)
        push!(refinement_order, index)
    end

    while true
        cancel_check()
        active = _ross_active_indices(accepted, refined, records)
        if isempty(active)
            return isempty(unresolved) ?
                finish(:complete_finite_policy, :frontier_exhausted) :
                finish(:unknown_gap, :frontier_exhausted_with_unresolved)
        end
        records_by_index = Dict(
            Tuple(record.multi_index) => record for record in records)
        ordered_active = sort!(copy(active); by=index -> (
            -records_by_index[index].indicator,
            sum(index),
            index,
        ))
        selected = first(ordered_active)
        max_indicator = records_by_index[selected].indicator
        if max_indicator <= plan.surplus_tolerance
            return isempty(unresolved) ?
                finish(:complete_finite_policy, :surplus_tolerance_met) :
                finish(:unknown_gap,
                    :surplus_tolerance_met_with_unresolved)
        end

        push!(refined, selected)
        push!(refinement_order, selected)
        candidates = Tuple[]
        level_blocked = true
        for axis in 1:dimension
            selected[axis] >= limits.max_level && continue
            level_blocked = false
            candidate = ntuple(position ->
                position == axis ? selected[position] + 1 : selected[position],
                dimension)
            candidate in accepted && continue
            _ross_is_admissible(candidate, accepted) || continue
            _ross_in_blocked_cone(candidate, blocked_cones) && continue
            push!(candidates, candidate)
        end
        sort!(unique!(candidates))
        if isempty(candidates)
            level_blocked && return finish(
                :partial_budget, :level_budget_exhausted)
            continue
        end
        for candidate in candidates
            reason = evaluate_index!(candidate)
            reason === nothing && continue
            reason == :unresolved_gap && continue
            return finish(:partial_budget, reason)
        end
    end
end
# Canonical, resumable RO-component adaptive transitions (v2).
#
# The v1 entry point above remains the compatibility surface.  The v2 surface
# makes the adaptive controller a pure state machine: a caller prepares one
# deterministic index batch, evaluates the requests elsewhere, and commits an
# exact ordered receipt.  No evaluator, closure, task, path, or backend handle
# is ever stored in a plan, state, batch, or result token.

const RO_SPARSE_RO_CHANNEL_PLAN_VERSION =
    "bne-ro-sparse-ro-channel-plan/v2.0.0"
const RO_SPARSE_RO_STATE_VERSION =
    "bne-ro-sparse-adaptive-state/v2.0.0"
const RO_SPARSE_RO_BATCH_VERSION =
    "bne-ro-sparse-index-batch/v2.0.0"
const RO_SPARSE_RO_RESULT_VERSION =
    "bne-ro-sparse-adaptive-result/v2.0.0"

struct _ROSparseV2Token end
const _ROSS_V2_TOKEN = _ROSparseV2Token()

"Canonical output-major, input-minor RO-channel sampling plan."
struct ROSparseROChannelPlanV2
    schema_version::String
    chart_id::String
    control_ids::Vector{String}
    source_coordinate_ids::Vector{String}
    chart_jacobian::Matrix{Float64}
    domain_lower::Vector{Float64}
    domain_upper::Vector{Float64}
    output_ids::Vector{String}
    fixed_background::Vector{Float64}
    surplus_tolerance::Float64
    initial_total_degree::Int
    limits::ROSparseSamplingLimits
    sampling_spec_sha256::String
    plan_sha256::String

    function ROSparseROChannelPlanV2(
        ::_ROSparseV2Token,
        schema_version,
        chart_id,
        control_ids,
        source_coordinate_ids,
        chart_jacobian,
        domain_lower,
        domain_upper,
        output_ids,
        fixed_background,
        surplus_tolerance,
        initial_total_degree,
        limits,
        sampling_spec_sha256,
        plan_sha256,
    )
        return new(
            String(schema_version), String(chart_id), copy(control_ids),
            copy(source_coordinate_ids), copy(chart_jacobian),
            copy(domain_lower), copy(domain_upper), copy(output_ids),
            copy(fixed_background), Float64(surplus_tolerance),
            Int(initial_total_degree), limits, String(sampling_spec_sha256),
            String(plan_sha256))
    end
end

"A self-hashed resumable state containing data only."
struct ROSparseAdaptiveStateV2
    schema_version::String
    plan_sha256::String
    sampling_spec_sha256::String
    initial_cursor::Int
    accepted_multi_indices::Vector{Vector{Int}}
    refinement_order::Vector{Vector{Int}}
    pending_candidates::Vector{Vector{Int}}
    index_records::Vector{ROSparseIndexRecord}
    samples::Vector{ROSparseSample}
    unresolved_regions::Vector{ROSparseUnresolvedRegion}
    interpolation_work_consumed::Int
    payload_scalar_count::Int
    backend_work_unit_count::Int
    state_sha256::String

    function ROSparseAdaptiveStateV2(
        ::_ROSparseV2Token,
        schema_version,
        plan_sha256,
        sampling_spec_sha256,
        initial_cursor,
        accepted_multi_indices,
        refinement_order,
        pending_candidates,
        index_records,
        samples,
        unresolved_regions,
        interpolation_work_consumed,
        payload_scalar_count,
        backend_work_unit_count,
        state_sha256,
        ; cancel_check=() -> nothing,
    )
        return new(
            String(schema_version), String(plan_sha256),
            String(sampling_spec_sha256), Int(initial_cursor),
            _ross_v2_copy_indices(accepted_multi_indices, cancel_check),
            _ross_v2_copy_indices(refinement_order, cancel_check),
            _ross_v2_copy_indices(pending_candidates, cancel_check),
            _ross_v2_copy_records(index_records, cancel_check),
            _ross_v2_copy_samples(samples, cancel_check),
            _ross_v2_copy_unresolved_regions(
                unresolved_regions, cancel_check),
            Int(interpolation_work_consumed), Int(payload_scalar_count),
            Int(backend_work_unit_count), String(state_sha256))
    end
end

"One exact, content-addressed unit of backend evaluation work."
struct ROSparseIndexBatchV2
    schema_version::String
    plan_sha256::String
    prior_state_sha256::String
    batch_ordinal::Int
    multi_index::Vector{Int}
    refinements_to_commit::Vector{Vector{Int}}
    initial_cursor_after::Int
    pending_candidates_after::Vector{Vector{Int}}
    requests::Vector{ROSparsePointRequest}
    point_count::Int
    payload_scalar_count::Int
    interpolation_work::Int
    batch_sha256::String

    function ROSparseIndexBatchV2(
        ::_ROSparseV2Token,
        schema_version,
        plan_sha256,
        prior_state_sha256,
        batch_ordinal,
        multi_index,
        refinements_to_commit,
        initial_cursor_after,
        pending_candidates_after,
        requests,
        point_count,
        payload_scalar_count,
        interpolation_work,
        batch_sha256,
        ; cancel_check=() -> nothing,
    )
        return new(
            String(schema_version), String(plan_sha256),
            String(prior_state_sha256), Int(batch_ordinal),
            Int.(multi_index),
            _ross_v2_copy_indices(refinements_to_commit, cancel_check),
            Int(initial_cursor_after),
            _ross_v2_copy_indices(pending_candidates_after, cancel_check),
            _ross_v2_copy_requests(requests, cancel_check),
            Int(point_count), Int(payload_scalar_count),
            Int(interpolation_work), String(batch_sha256))
    end
end

"Self-hashed terminal finite-policy result."
struct ROSparseSamplingResultV2
    schema_version::String
    plan_sha256::String
    sampling_spec_sha256::String
    terminal_state_sha256::String
    status::Symbol
    stopping_reason::Symbol
    evidence_scope::Symbol
    continuum_error_bound::Nothing
    accepted_multi_indices::Vector{Vector{Int}}
    active_frontier::Vector{Vector{Int}}
    refinement_order::Vector{Vector{Int}}
    index_records::Vector{ROSparseIndexRecord}
    samples::Vector{ROSparseSample}
    unresolved_regions::Vector{ROSparseUnresolvedRegion}
    evaluated_point_count::Int
    valid_point_count::Int
    invalid_point_count::Int
    interpolation_work_consumed::Int
    payload_scalar_count::Int
    backend_work_unit_count::Int
    max_active_indicator::Union{Nothing,Float64}
    result_sha256::String

    function ROSparseSamplingResultV2(
        ::_ROSparseV2Token,
        schema_version,
        plan_sha256,
        sampling_spec_sha256,
        terminal_state_sha256,
        status,
        stopping_reason,
        evidence_scope,
        accepted_multi_indices,
        active_frontier,
        refinement_order,
        index_records,
        samples,
        unresolved_regions,
        evaluated_point_count,
        valid_point_count,
        invalid_point_count,
        interpolation_work_consumed,
        payload_scalar_count,
        backend_work_unit_count,
        max_active_indicator,
        result_sha256,
        ; cancel_check=() -> nothing,
    )
        return new(
            String(schema_version), String(plan_sha256),
            String(sampling_spec_sha256), String(terminal_state_sha256),
            Symbol(status), Symbol(stopping_reason), Symbol(evidence_scope),
            nothing,
            _ross_v2_copy_indices(accepted_multi_indices, cancel_check),
            _ross_v2_copy_indices(active_frontier, cancel_check),
            _ross_v2_copy_indices(refinement_order, cancel_check),
            _ross_v2_copy_records(index_records, cancel_check),
            _ross_v2_copy_samples(samples, cancel_check),
            _ross_v2_copy_unresolved_regions(
                unresolved_regions, cancel_check),
            Int(evaluated_point_count), Int(valid_point_count),
            Int(invalid_point_count), Int(interpolation_work_consumed),
            Int(payload_scalar_count), Int(backend_work_unit_count),
            max_active_indicator === nothing ? nothing :
                Float64(max_active_indicator), String(result_sha256))
    end
end

function _ross_v2_copy_indices(raw, cancel_check)
    values = Vector{Vector{Int}}()
    sizehint!(values, length(raw))
    for (position, index) in enumerate(raw)
        (position == 1 || position % 256 == 0) && cancel_check()
        push!(values, Int.(index))
    end
    cancel_check()
    return values
end

function _ross_v2_copy_requests(raw, cancel_check)
    values = ROSparsePointRequest[]
    sizehint!(values, length(raw))
    for (position, request) in enumerate(raw)
        (position == 1 || position % 256 == 0) && cancel_check()
        push!(values, _ross_v2_copy_request(request))
    end
    cancel_check()
    return values
end

function _ross_v2_copy_records(raw, cancel_check)
    values = ROSparseIndexRecord[]
    sizehint!(values, length(raw))
    for (position, record) in enumerate(raw)
        (position == 1 || position % 256 == 0) && cancel_check()
        push!(values, _ross_v2_copy_record(record))
    end
    cancel_check()
    return values
end

function _ross_v2_copy_samples(raw, cancel_check)
    values = ROSparseSample[]
    sizehint!(values, length(raw))
    for (position, sample) in enumerate(raw)
        (position == 1 || position % 256 == 0) && cancel_check()
        push!(values, _ross_v2_copy_sample(sample))
    end
    cancel_check()
    return values
end

function _ross_v2_copy_unresolved_regions(raw, cancel_check)
    values = ROSparseUnresolvedRegion[]
    sizehint!(values, length(raw))
    for (position, region) in enumerate(raw)
        (position == 1 || position % 256 == 0) && cancel_check()
        push!(values, _ross_v2_copy_unresolved(region))
    end
    cancel_check()
    return values
end

function _ross_v2_all_cancel(predicate, raw, cancel_check)
    for (position, value) in enumerate(raw)
        (position == 1 || position % 256 == 0) && cancel_check()
        predicate(value) || return false
    end
    cancel_check()
    return true
end

_ross_v2_copy_evaluation(evaluation::ROSparseEvaluation) =
    ROSparseEvaluation(
        evaluation.valid,
        evaluation.values === nothing ? nothing : copy(evaluation.values),
        evaluation.invalid_reason)

_ross_v2_copy_request(request::ROSparsePointRequest) = ROSparsePointRequest(
    request.point_id, copy(request.multi_index), copy(request.node_ids),
    copy(request.normalized_coordinates), copy(request.control_coordinates),
    copy(request.source_coordinates))

_ross_v2_copy_sample(sample::ROSparseSample) = ROSparseSample(
    _ross_v2_copy_request(sample.request),
    _ross_v2_copy_evaluation(sample.evaluation),
    sample.surplus === nothing ? nothing : copy(sample.surplus))

_ross_v2_copy_record(record::ROSparseIndexRecord) = ROSparseIndexRecord(
    copy(record.multi_index), copy(record.point_ids), record.status,
    record.indicator)

_ross_v2_copy_unresolved(region::ROSparseUnresolvedRegion) =
    ROSparseUnresolvedRegion(
        copy(region.multi_index), copy(region.point_ids), copy(region.reasons))

_ross_v2_limit_payload(limits::ROSparseSamplingLimits) = (
    max_level=limits.max_level,
    max_points=limits.max_points,
    max_multi_indices=limits.max_multi_indices,
    max_interpolation_work=limits.max_work,
    max_payload_scalars=limits.max_payload_scalars,
)

function _ross_v2_component_payload(output_ids, control_ids)
    return Tuple((
        channel_index=(output_position - 1) * length(control_ids) +
            input_position,
        output_id=output_id,
        input_axis_id=input_id,
    ) for (output_position, output_id) in enumerate(output_ids)
        for (input_position, input_id) in enumerate(control_ids))
end

function _ross_v2_sampling_spec_body(
    chart_id,
    control_ids,
    source_coordinate_ids,
    chart_jacobian,
    domain_lower,
    domain_upper,
    output_ids,
    fixed_background,
    surplus_tolerance,
    initial_total_degree,
)
    jacobian_rows = Tuple(Tuple(Float64(chart_jacobian[row, column])
        for column in axes(chart_jacobian, 2))
        for row in axes(chart_jacobian, 1))
    return (
        schema_version=RO_SPARSE_RO_CHANNEL_PLAN_VERSION,
        chart=(
            chart_id=String(chart_id),
            control_ids=Tuple(String.(control_ids)),
            source_coordinate_ids=Tuple(String.(source_coordinate_ids)),
            jacobian=jacobian_rows,
        ),
        domain=(
            lower=Tuple(Float64.(domain_lower)),
            upper=Tuple(Float64.(domain_upper)),
        ),
        outputs=(
            output_ids=Tuple(String.(output_ids)),
            reaction_order_components=
                _ross_v2_component_payload(output_ids, control_ids),
        ),
        fixed_background=Tuple(Float64.(fixed_background)),
        policy=(
            initial_total_degree=Int(initial_total_degree),
            indicator=
                "ordered_ro_components_linf_hierarchical_surplus",
            channel_order="output_major_then_input_minor",
            frontier_order=
                "indicator_desc_total_level_asc_lexicographic",
            invalid_policy=
                "unresolved_cone_blocks_descendants_only",
            surplus_tolerance=Float64(surplus_tolerance),
            evidence_scope=String(RO_SPARSE_EVIDENCE_SCOPE),
            continuum_error_bound=nothing,
        ),
    )
end

function _ross_v2_plan_body(plan::ROSparseROChannelPlanV2)
    spec = _ross_v2_sampling_spec_body(
        plan.chart_id, plan.control_ids, plan.source_coordinate_ids,
        plan.chart_jacobian, plan.domain_lower, plan.domain_upper,
        plan.output_ids, plan.fixed_background, plan.surplus_tolerance,
        plan.initial_total_degree)
    return (
        schema_version=plan.schema_version,
        sampling_spec=spec,
        execution_budget=_ross_v2_limit_payload(plan.limits),
        portable_token_policy=(
            max_token_bytes=_ROSS_V2_MAX_PORTABLE_TOKEN_BYTES,
            enforcement=
                "all_v2_token_construction_publication_and_restore",
        ),
        sampling_spec_sha256=plan.sampling_spec_sha256,
    )
end

function ROSparseROChannelPlanV2(;
    chart_id,
    control_ids,
    source_coordinate_ids,
    chart_jacobian,
    domain_lower,
    domain_upper,
    output_ids,
    fixed_background,
    surplus_tolerance::Real=1e-8,
    initial_total_degree::Integer=2,
    limits::ROSparseSamplingLimits=ROSparseSamplingLimits(),
)
    checked_limits = _ross_normalize_limits(limits)
    controls = _ross_ids(
        control_ids, "control_ids", _ROSS_HARD_MAX_DIMENSIONS)
    outputs = _ross_ids(output_ids, "output_ids", _ROSS_HARD_MAX_OUTPUTS)
    channel_count = length(controls) * length(outputs)
    channel_count <= _ROSS_HARD_MAX_OUTPUTS || throw(ArgumentError(
        "output_count * input_count must not exceed " *
        string(_ROSS_HARD_MAX_OUTPUTS) * " ordered RO components"))
    channel_ids = ["ro_component[$output_position,$input_position]"
        for output_position in eachindex(outputs)
        for input_position in eachindex(controls)]
    checked = ROSparseSamplingPlan(
        chart_id=chart_id,
        control_ids=controls,
        source_coordinate_ids=source_coordinate_ids,
        chart_jacobian=chart_jacobian,
        domain_lower=domain_lower,
        domain_upper=domain_upper,
        output_ids=channel_ids,
        fixed_background=fixed_background,
        surplus_tolerance=surplus_tolerance,
        initial_total_degree=initial_total_degree)
    spec = _ross_v2_sampling_spec_body(
        checked.chart_id, checked.control_ids,
        checked.source_coordinate_ids, checked.chart_jacobian,
        checked.domain_lower, checked.domain_upper, outputs,
        checked.fixed_background, checked.surplus_tolerance,
        checked.initial_total_degree)
    spec_hash = _ross_sha256(spec)
    provisional = ROSparseROChannelPlanV2(
        _ROSS_V2_TOKEN, RO_SPARSE_RO_CHANNEL_PLAN_VERSION,
        checked.chart_id, checked.control_ids,
        checked.source_coordinate_ids, checked.chart_jacobian,
        checked.domain_lower, checked.domain_upper, outputs,
        checked.fixed_background, checked.surplus_tolerance,
        checked.initial_total_degree, checked_limits, spec_hash, "")
    plan_hash = _ross_sha256(_ross_v2_plan_body(provisional))
    plan = ROSparseROChannelPlanV2(
        _ROSS_V2_TOKEN, provisional.schema_version,
        provisional.chart_id, provisional.control_ids,
        provisional.source_coordinate_ids, provisional.chart_jacobian,
        provisional.domain_lower, provisional.domain_upper,
        provisional.output_ids, provisional.fixed_background,
        provisional.surplus_tolerance, provisional.initial_total_degree,
        provisional.limits, provisional.sampling_spec_sha256, plan_hash)
    _ross_v2_assert_portable_token_size(
        merge(_ross_v2_plan_body(plan), (plan_sha256=plan.plan_sha256,)),
        "plan")
    return plan
end

ro_sparse_ro_channel_order_v2(plan::ROSparseROChannelPlanV2) =
    collect(_ross_v2_component_payload(plan.output_ids, plan.control_ids))

ro_sparse_portable_token_byte_limit_v2(::ROSparseROChannelPlanV2) =
    _ROSS_V2_MAX_PORTABLE_TOKEN_BYTES

function ro_sparse_ro_channel_plan_v2_payload(
    plan::ROSparseROChannelPlanV2;
    cancel_check=() -> nothing,
)
    validate_ro_sparse_ro_channel_plan_v2(plan)
    payload = merge(
        _ross_v2_plan_body(plan), (plan_sha256=plan.plan_sha256,))
    _ross_v2_assert_portable_token_size(payload, "plan", cancel_check)
    return payload
end

function _ross_v2_legacy_plan(plan::ROSparseROChannelPlanV2)
    channel_ids = ["ro_component[$output_position,$input_position]"
        for output_position in eachindex(plan.output_ids)
        for input_position in eachindex(plan.control_ids)]
    return ROSparseSamplingPlan(
        chart_id=plan.chart_id,
        control_ids=plan.control_ids,
        source_coordinate_ids=plan.source_coordinate_ids,
        chart_jacobian=plan.chart_jacobian,
        domain_lower=plan.domain_lower,
        domain_upper=plan.domain_upper,
        output_ids=channel_ids,
        fixed_background=plan.fixed_background,
        surplus_tolerance=plan.surplus_tolerance,
        initial_total_degree=plan.initial_total_degree)
end

function validate_ro_sparse_ro_channel_plan_v2(
    plan::ROSparseROChannelPlanV2)
    plan.schema_version == RO_SPARSE_RO_CHANNEL_PLAN_VERSION ||
        throw(ArgumentError("unsupported v2 RO-channel plan version"))
    normalized = ROSparseROChannelPlanV2(
        chart_id=plan.chart_id,
        control_ids=plan.control_ids,
        source_coordinate_ids=plan.source_coordinate_ids,
        chart_jacobian=plan.chart_jacobian,
        domain_lower=plan.domain_lower,
        domain_upper=plan.domain_upper,
        output_ids=plan.output_ids,
        fixed_background=plan.fixed_background,
        surplus_tolerance=plan.surplus_tolerance,
        initial_total_degree=plan.initial_total_degree,
        limits=plan.limits)
    plan.sampling_spec_sha256 == normalized.sampling_spec_sha256 ||
        throw(ArgumentError("v2 RO-channel sampling-spec hash mismatch"))
    plan.plan_sha256 == normalized.plan_sha256 ||
        throw(ArgumentError("v2 RO-channel plan hash mismatch"))
    JSON3.write(_ross_v2_plan_body(plan)) ==
        JSON3.write(_ross_v2_plan_body(normalized)) || throw(ArgumentError(
            "v2 RO-channel plan is not canonical"))
    return true
end

function _ross_v2_request_payload(request::ROSparsePointRequest)
    return (
        point_id=request.point_id,
        multi_index=Tuple(request.multi_index),
        node_ids=Tuple(request.node_ids),
        normalized_coordinates=Tuple(request.normalized_coordinates),
        control_coordinates=Tuple(request.control_coordinates),
        source_coordinates=Tuple(request.source_coordinates),
    )
end

function _ross_v2_sample_payload(sample::ROSparseSample)
    return merge(_ross_v2_request_payload(sample.request), (
        valid=sample.evaluation.valid,
        values=sample.evaluation.values === nothing ? nothing :
            Tuple(sample.evaluation.values),
        invalid_reason=sample.evaluation.invalid_reason === nothing ?
            nothing : String(sample.evaluation.invalid_reason),
        surplus=sample.surplus === nothing ? nothing : Tuple(sample.surplus),
    ))
end

_ross_v2_record_payload(record::ROSparseIndexRecord) = (
    multi_index=Tuple(record.multi_index),
    point_ids=Tuple(record.point_ids),
    status=String(record.status),
    indicator=record.indicator,
)

_ross_v2_unresolved_payload(region::ROSparseUnresolvedRegion) = (
    multi_index=Tuple(region.multi_index),
    point_ids=Tuple(region.point_ids),
    reasons=Tuple(String.(region.reasons)),
)

function _ross_v2_state_body(state::ROSparseAdaptiveStateV2)
    return (
        schema_version=state.schema_version,
        plan_sha256=state.plan_sha256,
        sampling_spec_sha256=state.sampling_spec_sha256,
        initial_cursor=state.initial_cursor,
        accepted_multi_indices=
            Tuple(Tuple(index) for index in state.accepted_multi_indices),
        refinement_order=
            Tuple(Tuple(index) for index in state.refinement_order),
        pending_candidates=
            Tuple(Tuple(index) for index in state.pending_candidates),
        index_records=Tuple(_ross_v2_record_payload(record)
            for record in state.index_records),
        samples=Tuple(_ross_v2_sample_payload(sample)
            for sample in state.samples),
        unresolved_regions=Tuple(_ross_v2_unresolved_payload(region)
            for region in state.unresolved_regions),
        counters=(
            interpolation_work_consumed=
                state.interpolation_work_consumed,
            payload_scalar_count=state.payload_scalar_count,
            backend_work_unit_count=state.backend_work_unit_count,
        ),
    )
end

function _ross_v2_new_state(
    plan::ROSparseROChannelPlanV2;
    initial_cursor=1,
    accepted_multi_indices=Vector{Vector{Int}}(),
    refinement_order=Vector{Vector{Int}}(),
    pending_candidates=Vector{Vector{Int}}(),
    index_records=ROSparseIndexRecord[],
    samples=ROSparseSample[],
    unresolved_regions=ROSparseUnresolvedRegion[],
    interpolation_work_consumed=0,
    payload_scalar_count=0,
    backend_work_unit_count=0,
    cancel_check=() -> nothing,
)
    provisional = ROSparseAdaptiveStateV2(
        _ROSS_V2_TOKEN, RO_SPARSE_RO_STATE_VERSION,
        plan.plan_sha256, plan.sampling_spec_sha256, initial_cursor,
        accepted_multi_indices, refinement_order, pending_candidates,
        index_records, samples, unresolved_regions,
        interpolation_work_consumed, payload_scalar_count,
        backend_work_unit_count, ""; cancel_check=cancel_check)
    cancel_check()
    state_hash = _ross_sha256(_ross_v2_state_body(provisional))
    cancel_check()
    state = ROSparseAdaptiveStateV2(
        _ROSS_V2_TOKEN, provisional.schema_version,
        provisional.plan_sha256, provisional.sampling_spec_sha256,
        provisional.initial_cursor, provisional.accepted_multi_indices,
        provisional.refinement_order, provisional.pending_candidates,
        provisional.index_records, provisional.samples,
        provisional.unresolved_regions,
        provisional.interpolation_work_consumed,
        provisional.payload_scalar_count,
        provisional.backend_work_unit_count, state_hash;
        cancel_check=cancel_check)
    _ross_v2_assert_portable_token_size(
        merge(_ross_v2_state_body(state),
            (state_sha256=state.state_sha256,)),
        "state", cancel_check)
    return state
end

function initialize_ro_sparse_state_v2(
    plan::ROSparseROChannelPlanV2;
    cancel_check=() -> nothing,
)
    cancel_check()
    validate_ro_sparse_ro_channel_plan_v2(plan)
    state = _ross_v2_new_state(plan; cancel_check=cancel_check)
    cancel_check()
    return state
end

function ro_sparse_state_v2_payload(
    state::ROSparseAdaptiveStateV2;
    cancel_check=() -> nothing,
)
    payload = merge(
        _ross_v2_state_body(state), (state_sha256=state.state_sha256,))
    _ross_v2_assert_portable_token_size(payload, "state", cancel_check)
    return payload
end

function _ross_v2_batch_body(batch::ROSparseIndexBatchV2)
    return (
        schema_version=batch.schema_version,
        plan_sha256=batch.plan_sha256,
        prior_state_sha256=batch.prior_state_sha256,
        batch_ordinal=batch.batch_ordinal,
        multi_index=Tuple(batch.multi_index),
        refinements_to_commit=
            Tuple(Tuple(index) for index in batch.refinements_to_commit),
        initial_cursor_after=batch.initial_cursor_after,
        pending_candidates_after=Tuple(
            Tuple(index) for index in batch.pending_candidates_after),
        requests=Tuple(_ross_v2_request_payload(request)
            for request in batch.requests),
        cost=(
            point_count=batch.point_count,
            payload_scalar_count=batch.payload_scalar_count,
            interpolation_work=batch.interpolation_work,
            backend_work_units=1,
        ),
    )
end

function _ross_v2_make_batch(
    plan::ROSparseROChannelPlanV2,
    state::ROSparseAdaptiveStateV2,
    index::Tuple,
    refinements,
    cursor_after::Int,
    pending_after,
    cancel_check,
)
    cancel_check()
    legacy_plan = _ross_v2_legacy_plan(plan)
    requests = ROSparsePointRequest[]
    axes = [_ross_increment_nodes(level) for level in index]
    _ross_each_product(axes) do nodes
        cancel_check()
        push!(requests, _ross_point_request(
            legacy_plan, plan.sampling_spec_sha256, index, nodes))
    end
    cancel_check()
    point_count, payload, work = _ross_candidate_cost(
        index, length(plan.output_ids) * length(plan.control_ids))
    provisional = ROSparseIndexBatchV2(
        _ROSS_V2_TOKEN, RO_SPARSE_RO_BATCH_VERSION, plan.plan_sha256,
        state.state_sha256, state.backend_work_unit_count + 1,
        collect(index), [collect(item) for item in refinements],
        cursor_after, [collect(item) for item in pending_after], requests,
        Int(point_count), Int(payload), Int(work), "";
        cancel_check=cancel_check)
    cancel_check()
    batch_hash = _ross_sha256(_ross_v2_batch_body(provisional))
    cancel_check()
    batch = ROSparseIndexBatchV2(
        _ROSS_V2_TOKEN, provisional.schema_version,
        provisional.plan_sha256, provisional.prior_state_sha256,
        provisional.batch_ordinal, provisional.multi_index,
        provisional.refinements_to_commit,
        provisional.initial_cursor_after,
        provisional.pending_candidates_after, provisional.requests,
        provisional.point_count, provisional.payload_scalar_count,
        provisional.interpolation_work, batch_hash;
        cancel_check=cancel_check)
    _ross_v2_assert_portable_token_size(
        merge(_ross_v2_batch_body(batch),
            (batch_sha256=batch.batch_sha256,)),
        "batch", cancel_check)
    return batch
end

function ro_sparse_index_batch_v2_payload(
    batch::ROSparseIndexBatchV2;
    cancel_check=() -> nothing,
)
    payload = merge(
        _ross_v2_batch_body(batch), (batch_sha256=batch.batch_sha256,))
    _ross_v2_assert_portable_token_size(payload, "batch", cancel_check)
    return payload
end

function _ross_v2_scheduler(
    plan::ROSparseROChannelPlanV2,
    state::ROSparseAdaptiveStateV2,
    cancel_check=() -> nothing,
)
    cancel_check()
    dimension = length(plan.control_ids)
    accepted = Set{Tuple}(Tuple(index)
        for index in state.accepted_multi_indices)
    blocked = Set{Tuple}(Tuple(region.multi_index)
        for region in state.unresolved_regions)
    refinements = Tuple[Tuple(index) for index in state.refinement_order]
    refined = Set(refinements)
    refinements_to_commit = Tuple[]
    pending = Tuple[Tuple(index) for index in state.pending_candidates]
    cursor = state.initial_cursor
    initial = _ross_initial_indices(dimension, plan.initial_total_degree)
    output_count = length(plan.output_ids) * length(plan.control_ids)

    function candidate_or_terminal(index::Tuple, pending_after)
        cancel_check()
        reason = _ross_budget_reason(
            index, length(state.samples), length(accepted),
            state.interpolation_work_consumed,
            state.payload_scalar_count, output_count, plan.limits)
        if reason !== nothing
            return (batch=nothing, status=:partial_budget, reason=reason,
                refinements=copy(refinements), cursor=cursor,
                pending=copy(pending_after))
        end
            return (batch=_ross_v2_make_batch(
                plan, state, index, refinements_to_commit,
                cursor, pending_after, cancel_check),
            status=nothing, reason=nothing, refinements=copy(refinements),
            cursor=cursor, pending=copy(pending_after))
    end

    while !isempty(pending)
        cancel_check()
        index = popfirst!(pending)
        index in accepted && continue
        _ross_in_blocked_cone(index, blocked) && continue
        return candidate_or_terminal(index, pending)
    end

    while cursor <= length(initial)
        cancel_check()
        index = initial[cursor]
        cursor += 1
        index in accepted && continue
        _ross_in_blocked_cone(index, blocked) && continue
        return candidate_or_terminal(index, Tuple[])
    end

    records_by_index = Dict(Tuple(record.multi_index) => record
        for record in state.index_records)
    while true
        cancel_check()
        active = sort!([index for index in accepted
            if !(index in refined) &&
                get(records_by_index, index, nothing) !== nothing &&
                records_by_index[index].indicator !== nothing])
        if isempty(active)
            return (batch=nothing,
                status=isempty(state.unresolved_regions) ?
                    :complete_finite_policy : :unknown_gap,
                reason=isempty(state.unresolved_regions) ?
                    :frontier_exhausted :
                    :frontier_exhausted_with_unresolved,
                refinements=copy(refinements), cursor=cursor,
                pending=Tuple[])
        end
        sort!(active; by=index -> (
            -records_by_index[index].indicator, sum(index), index))
        selected = first(active)
        if records_by_index[selected].indicator <= plan.surplus_tolerance
            return (batch=nothing,
                status=isempty(state.unresolved_regions) ?
                    :complete_finite_policy : :unknown_gap,
                reason=isempty(state.unresolved_regions) ?
                    :surplus_tolerance_met :
                    :surplus_tolerance_met_with_unresolved,
                refinements=copy(refinements), cursor=cursor,
                pending=Tuple[])
        end
        push!(refined, selected)
        push!(refinements, selected)
        push!(refinements_to_commit, selected)
        candidates = Tuple[]
        level_blocked = true
        for axis in 1:dimension
            cancel_check()
            selected[axis] >= plan.limits.max_level && continue
            level_blocked = false
            candidate = ntuple(position -> position == axis ?
                selected[position] + 1 : selected[position], dimension)
            candidate in accepted && continue
            _ross_is_admissible(candidate, accepted) || continue
            _ross_in_blocked_cone(candidate, blocked) && continue
            push!(candidates, candidate)
        end
        sort!(unique!(candidates))
        if isempty(candidates)
            if level_blocked
                return (batch=nothing, status=:partial_budget,
                    reason=:level_budget_exhausted,
                    refinements=copy(refinements), cursor=cursor,
                    pending=Tuple[])
            end
            continue
        end
        candidate = popfirst!(candidates)
        return candidate_or_terminal(candidate, candidates)
    end
end

function _ross_v2_validate_state_shallow(
    plan::ROSparseROChannelPlanV2,
    state::ROSparseAdaptiveStateV2,
    cancel_check=() -> nothing,
)
    cancel_check()
    state.schema_version == RO_SPARSE_RO_STATE_VERSION ||
        throw(ArgumentError("unsupported v2 sparse state version"))
    state.plan_sha256 == plan.plan_sha256 || throw(ArgumentError(
        "v2 sparse state belongs to a different plan"))
    state.sampling_spec_sha256 == plan.sampling_spec_sha256 ||
        throw(ArgumentError("v2 sparse state sampling identity mismatch"))
    dimension = length(plan.control_ids)
    initial_count = length(_ross_initial_indices(
        dimension, plan.initial_total_degree))
    1 <= state.initial_cursor <= initial_count + 1 || throw(ArgumentError(
        "v2 sparse state initial cursor is out of range"))
    length(state.accepted_multi_indices) <= plan.limits.max_multi_indices ||
        throw(ArgumentError(
            "v2 sparse state exceeds the multi-index budget"))
    length(state.samples) <= plan.limits.max_points || throw(ArgumentError(
        "v2 sparse state exceeds the point budget"))
    length(state.index_records) <= plan.limits.max_multi_indices ||
        throw(ArgumentError(
            "v2 sparse state exceeds the record budget"))
    length(state.refinement_order) <= plan.limits.max_multi_indices ||
        throw(ArgumentError(
            "v2 sparse state exceeds the refinement budget"))
    length(state.unresolved_regions) <= plan.limits.max_multi_indices ||
        throw(ArgumentError(
            "v2 sparse state exceeds the unresolved-region budget"))
    length(state.pending_candidates) <= dimension || throw(ArgumentError(
        "v2 sparse state exceeds the pending-candidate bound"))
    accepted = Tuple[]
    sizehint!(accepted, length(state.accepted_multi_indices))
    for (position, index) in enumerate(state.accepted_multi_indices)
        _ross_v2_cancel_checkpoint(cancel_check, position)
        push!(accepted, Tuple(index))
    end
    _ross_v2_all_cancel(index -> length(index) == dimension &&
            all(level -> 1 <= level <= plan.limits.max_level, index),
        accepted, cancel_check) ||
        throw(ArgumentError("v2 sparse state has an invalid multi-index"))
    allunique(accepted) || throw(ArgumentError(
        "v2 sparse state repeats an accepted multi-index"))
    isempty(accepted) || smolyak_is_downward_closed(accepted) ||
        throw(ArgumentError("v2 sparse state is not downward closed"))
    length(state.index_records) == length(accepted) || throw(ArgumentError(
        "v2 sparse state record count does not match accepted indices"))
    Tuple.(getfield.(state.index_records, :multi_index)) == accepted ||
        throw(ArgumentError("v2 sparse state record order is not canonical"))
    state.backend_work_unit_count == length(accepted) || throw(ArgumentError(
        "backend work-unit count must equal committed index batches"))
    output_count = length(plan.output_ids) * length(plan.control_ids)
    expected_work = BigInt(0)
    expected_payload = BigInt(0)
    expected_samples = BigInt(0)
    unresolved_indices = Tuple[]
    sample_cursor = 1
    for (record_position, record) in enumerate(state.index_records)
        _ross_v2_cancel_checkpoint(cancel_check, record_position)
        index = Tuple(record.multi_index)
        points, payload, work = _ross_candidate_cost(index, output_count)
        expected_work += work
        expected_payload += payload
        expected_samples += points
        expected_work <= plan.limits.max_work || throw(ArgumentError(
            "v2 sparse state exceeds the interpolation-work budget"))
        expected_payload <= plan.limits.max_payload_scalars ||
            throw(ArgumentError(
                "v2 sparse state exceeds the payload-scalar budget"))
        expected_samples <= plan.limits.max_points || throw(ArgumentError(
            "v2 sparse state exceeds the point budget"))
        BigInt(length(record.point_ids)) == points || throw(ArgumentError(
            "v2 sparse record point count is inconsistent"))
        stop = sample_cursor + Int(points) - 1
        stop <= length(state.samples) || throw(ArgumentError(
            "v2 sparse state is missing record samples"))
        group = @view state.samples[sample_cursor:stop]
        Tuple(record.point_ids) == Tuple(
            sample.request.point_id for sample in group) ||
            throw(ArgumentError(
                "v2 sparse record sample order is inconsistent"))
        _ross_v2_all_cancel(
            sample -> Tuple(sample.request.multi_index) == index,
            group, cancel_check) ||
            throw(ArgumentError("v2 sparse record mixes multi-indices"))
        if record.status == :resolved
            record.indicator !== nothing && isfinite(record.indicator) &&
                record.indicator >= 0.0 || throw(ArgumentError(
                    "resolved v2 sparse records require an indicator"))
            _ross_v2_all_cancel(sample -> sample.evaluation.valid &&
                    sample.surplus !== nothing &&
                    length(sample.evaluation.values) == output_count &&
                    length(sample.surplus) == output_count,
                group, cancel_check) ||
                throw(ArgumentError(
                    "resolved v2 sparse samples are incomplete"))
        elseif record.status == :unresolved_gap
            record.indicator === nothing || throw(ArgumentError(
                "unresolved v2 sparse records cannot have an indicator"))
            !_ross_v2_all_cancel(
                sample -> sample.evaluation.valid, group, cancel_check) ||
                throw(ArgumentError(
                    "unresolved v2 sparse records need an invalid sample"))
            _ross_v2_all_cancel(
                sample -> sample.surplus === nothing,
                group, cancel_check) ||
                throw(ArgumentError(
                    "unresolved v2 sparse samples cannot carry surplus"))
            push!(unresolved_indices, index)
        else
            throw(ArgumentError("invalid v2 sparse record status"))
        end
        sample_cursor = stop + 1
    end
    expected_samples == BigInt(length(state.samples)) || throw(ArgumentError(
        "v2 sparse state has unowned samples"))
    expected_work == BigInt(state.interpolation_work_consumed) ||
        throw(ArgumentError("v2 interpolation-work counter mismatch"))
    expected_payload == BigInt(state.payload_scalar_count) ||
        throw(ArgumentError("v2 payload-scalar counter mismatch"))
    Tuple.(getfield.(state.unresolved_regions, :multi_index)) ==
        unresolved_indices || throw(ArgumentError(
            "v2 unresolved-region order is inconsistent"))
    refined = Tuple[Tuple(index) for index in state.refinement_order]
    length(refined) <= plan.limits.max_multi_indices &&
        allunique(refined) &&
        _ross_v2_all_cancel(index -> index in accepted, refined, cancel_check) ||
        throw(ArgumentError("v2 refinement order is inconsistent"))
    records_by_index = Dict(Tuple(record.multi_index) => record
        for record in state.index_records)
    all(index -> records_by_index[index].indicator !== nothing, refined) ||
        throw(ArgumentError("an unresolved index cannot be refined"))
    pending = Tuple[Tuple(index) for index in state.pending_candidates]
    length(pending) <= dimension && allunique(pending) &&
        _ross_v2_all_cancel(index -> length(index) == dimension &&
            all(level -> 1 <= level <= plan.limits.max_level, index) &&
            !(index in accepted), pending, cancel_check) || throw(ArgumentError(
        "v2 pending candidate queue is inconsistent"))
    cancel_check()
    state.state_sha256 == _ross_sha256(_ross_v2_state_body(state)) ||
        throw(ArgumentError("v2 sparse state hash mismatch"))
    cancel_check()
    return true
end

function _ross_v2_eval_receipt(
    request::ROSparsePointRequest,
    evaluation::ROSparseEvaluation,
)
    if evaluation.valid
        evaluation.values === nothing && throw(ArgumentError(
            "valid ordered evaluation requires values"))
        values = _ross_finite_vector(
            evaluation.values, "ordered evaluation values")
        evaluation.invalid_reason === nothing || throw(ArgumentError(
            "valid ordered evaluation cannot carry an invalid reason"))
        return (point_id=request.point_id, status="valid",
            values=Tuple(values), invalid_reason=nothing)
    end
    evaluation.values === nothing || throw(ArgumentError(
        "invalid ordered evaluation cannot carry values"))
    evaluation.invalid_reason === nothing && throw(ArgumentError(
        "invalid ordered evaluation requires a reason"))
    reason = String(evaluation.invalid_reason)
    isempty(reason) && throw(ArgumentError("invalid reason is empty"))
    ncodeunits(reason) <= 128 || throw(ArgumentError(
        "invalid reason must not exceed 128 UTF-8 bytes"))
    return (point_id=request.point_id, status="invalid", values=nothing,
        invalid_reason=reason)
end

ro_sparse_ordered_evaluation_v2(
    request::ROSparsePointRequest,
    evaluation::ROSparseEvaluation,
) = _ross_v2_eval_receipt(request, evaluation)

function _ross_v2_receipt_get(receipt, name::Symbol)
    if receipt isa NamedTuple
        hasproperty(receipt, name) || throw(ArgumentError(
            "ordered evaluation is missing $(name)"))
        return getproperty(receipt, name)
    elseif receipt isa AbstractDict
        haskey(receipt, name) && return receipt[name]
        haskey(receipt, String(name)) && return receipt[String(name)]
    else
        hasproperty(receipt, name) && return getproperty(receipt, name)
        try
            return receipt[name]
        catch
            try
                return receipt[String(name)]
            catch
            end
        end
    end
    throw(ArgumentError("ordered evaluation is missing $(name)"))
end

function _ross_v2_receipt_keys(receipt)
    raw = receipt isa NamedTuple ? propertynames(receipt) : keys(receipt)
    return Set(String(key) for key in raw)
end

function _ross_v2_normalize_evaluations(
    batch,
    raw_evaluations,
    output_count,
    cancel_check=() -> nothing,
)
    cancel_check()
    (raw_evaluations isa AbstractVector || raw_evaluations isa Tuple) ||
        throw(ArgumentError(
            "ordered evaluations must be an ordered collection"))
    length(raw_evaluations) == length(batch.requests) ||
        throw(ArgumentError(
            "ordered evaluations must exactly cover the batch"))
    expected_keys = Set(["point_id", "status", "values", "invalid_reason"])
    point_ids = String[]
    evaluations = ROSparseEvaluation[]
    for (position, raw) in enumerate(raw_evaluations)
        _ross_v2_cancel_checkpoint(cancel_check, position)
        _ross_v2_receipt_keys(raw) == expected_keys || throw(ArgumentError(
            "ordered evaluation fields are not canonical"))
        point_id_raw = _ross_v2_receipt_get(raw, :point_id)
        point_id_raw isa AbstractString || throw(ArgumentError(
            "ordered evaluation point_id must be a string"))
        push!(point_ids, String(point_id_raw))
        status_raw = _ross_v2_receipt_get(raw, :status)
        status_raw isa AbstractString || throw(ArgumentError(
            "ordered evaluation status must be a string"))
        status = String(status_raw)
        values_raw = _ross_v2_receipt_get(raw, :values)
        reason_raw = _ross_v2_receipt_get(raw, :invalid_reason)
        if status == "valid"
            reason_raw === nothing || throw(ArgumentError(
                "valid ordered evaluation cannot carry invalid_reason"))
            (values_raw isa AbstractVector || values_raw isa Tuple) ||
                throw(ArgumentError(
                    "valid ordered evaluation requires a value vector"))
            values = _ross_finite_vector(
                collect(values_raw), "ordered evaluation values")
            length(values) == output_count || throw(DimensionMismatch(
                "ordered RO-component count does not match the plan"))
            push!(evaluations, ROSparseEvaluation(true, values, nothing))
        elseif status == "invalid"
            values_raw === nothing || throw(ArgumentError(
                "invalid ordered evaluation cannot carry values"))
            reason_raw isa AbstractString || throw(ArgumentError(
                "invalid ordered evaluation requires a string reason"))
            reason = String(reason_raw)
            isempty(reason) && throw(ArgumentError(
                "invalid ordered evaluation reason must not be empty"))
            ncodeunits(reason) <= 128 || throw(ArgumentError(
                "invalid reason must not exceed 128 UTF-8 bytes"))
            push!(evaluations,
                ROSparseEvaluation(false, nothing, Symbol(reason)))
        else
            throw(ArgumentError(
                "ordered evaluation status must be valid or invalid"))
        end
    end
    allunique(point_ids) || throw(ArgumentError(
        "ordered evaluations contain duplicate point_ids"))
    expected = [request.point_id for request in batch.requests]
    Set(point_ids) == Set(expected) || throw(ArgumentError(
        "ordered evaluations contain foreign or missing point_ids"))
    point_ids == expected || throw(ArgumentError(
        "ordered evaluations are out of request order"))
    cancel_check()
    return evaluations
end

function _ross_v2_commit_unchecked(
    plan::ROSparseROChannelPlanV2,
    state::ROSparseAdaptiveStateV2,
    batch::ROSparseIndexBatchV2,
    evaluations::Vector{ROSparseEvaluation},
    cancel_check=() -> nothing,
)
    cancel_check()
    index = Tuple(batch.multi_index)
    output_count = length(plan.output_ids) * length(plan.control_ids)
    all_evaluations = Dict{String,ROSparseEvaluation}()
    for sample in state.samples
        cancel_check()
        all_evaluations[sample.request.point_id] =
            _ross_v2_copy_evaluation(sample.evaluation)
    end
    for (request, evaluation) in zip(batch.requests, evaluations)
        cancel_check()
        haskey(all_evaluations, request.point_id) && error(
            "v2 sparse point identity was committed twice")
        all_evaluations[request.point_id] =
            _ross_v2_copy_evaluation(evaluation)
    end
    accepted = [copy(item) for item in state.accepted_multi_indices]
    push!(accepted, collect(index))
    refinements = [copy(item) for item in state.refinement_order]
    for item in batch.refinements_to_commit
        item in refinements || push!(refinements, copy(item))
    end
    records = [_ross_v2_copy_record(record)
        for record in state.index_records]
    samples = [_ross_v2_copy_sample(sample) for sample in state.samples]
    unresolved = [_ross_v2_copy_unresolved(region)
        for region in state.unresolved_regions]
    point_ids = [request.point_id for request in batch.requests]
    invalid_positions = findall(evaluation -> !evaluation.valid, evaluations)
    if isempty(invalid_positions)
        surpluses = Vector{Vector{Float64}}()
        for request in batch.requests
            cancel_check()
            surplus = _ross_mixed_surplus(
                index, request, all_evaluations, output_count,
                plan.sampling_spec_sha256)
            surplus === nothing && error(
                "valid downward-closed v2 candidate lacks a surplus")
            push!(surpluses, surplus)
        end
        cancel_check()
        indicator = maximum((maximum(abs, surplus)
            for surplus in surpluses); init=0.0)
        for position in eachindex(batch.requests)
            cancel_check()
            push!(samples, ROSparseSample(
                _ross_v2_copy_request(batch.requests[position]),
                _ross_v2_copy_evaluation(evaluations[position]),
                copy(surpluses[position])))
        end
        push!(records, ROSparseIndexRecord(
            collect(index), point_ids, :resolved, indicator))
        if sum(index) - length(index) < plan.initial_total_degree &&
                !(collect(index) in refinements)
            push!(refinements, collect(index))
        end
    else
        for position in eachindex(batch.requests)
            cancel_check()
            push!(samples, ROSparseSample(
                _ross_v2_copy_request(batch.requests[position]),
                _ross_v2_copy_evaluation(evaluations[position]), nothing))
        end
        reasons = sort!(unique(Symbol[
            evaluations[position].invalid_reason
            for position in invalid_positions]); by=string)
        push!(records, ROSparseIndexRecord(
            collect(index), point_ids, :unresolved_gap, nothing))
        push!(unresolved, ROSparseUnresolvedRegion(
            collect(index), point_ids, reasons))
    end
    cancel_check()
    next_state = _ross_v2_new_state(
        plan,
        initial_cursor=batch.initial_cursor_after,
        accepted_multi_indices=accepted,
        refinement_order=refinements,
        pending_candidates=batch.pending_candidates_after,
        index_records=records,
        samples=samples,
        unresolved_regions=unresolved,
        interpolation_work_consumed=
            state.interpolation_work_consumed + batch.interpolation_work,
        payload_scalar_count=
            state.payload_scalar_count + batch.payload_scalar_count,
        backend_work_unit_count=state.backend_work_unit_count + 1,
        cancel_check=cancel_check)
    cancel_check()
    return next_state
end

function _ross_v2_replay_state(
    plan::ROSparseROChannelPlanV2,
    expected::ROSparseAdaptiveStateV2,
    cancel_check,
)
    replay = _ross_v2_new_state(plan; cancel_check=cancel_check)
    sample_cursor = 1
    for expected_record in expected.index_records
        cancel_check()
        schedule = _ross_v2_scheduler(plan, replay, cancel_check)
        schedule.batch === nothing && throw(ArgumentError(
            "v2 sparse state contains a commit after a terminal policy state"))
        batch = schedule.batch
        Tuple(batch.multi_index) == Tuple(expected_record.multi_index) ||
            throw(ArgumentError(
                "v2 sparse state commit order cannot be replayed"))
        count = length(batch.requests)
        stop = sample_cursor + count - 1
        stop <= length(expected.samples) || throw(ArgumentError(
            "v2 sparse replay is missing samples"))
        group = @view expected.samples[sample_cursor:stop]
        receipts = NamedTuple[]
        sizehint!(receipts, count)
        for position in eachindex(batch.requests)
            _ross_v2_cancel_checkpoint(cancel_check, position)
            push!(receipts, _ross_v2_eval_receipt(
                batch.requests[position], group[position].evaluation))
        end
        evaluations = _ross_v2_normalize_evaluations(
            batch, receipts,
            length(plan.output_ids) * length(plan.control_ids), cancel_check)
        replay = _ross_v2_commit_unchecked(
            plan, replay, batch, evaluations, cancel_check)
        sample_cursor = stop + 1
    end
    JSON3.write(_ross_v2_state_body(replay)) ==
        JSON3.write(_ross_v2_state_body(expected)) || throw(ArgumentError(
            "v2 sparse state is self-consistent but not a canonical replay"))
    replay.state_sha256 == expected.state_sha256 || throw(ArgumentError(
        "v2 sparse state replay hash mismatch"))
    return true
end

function validate_ro_sparse_state_v2(
    plan::ROSparseROChannelPlanV2,
    state::ROSparseAdaptiveStateV2;
    cancel_check=() -> nothing,
)
    cancel_check()
    validate_ro_sparse_ro_channel_plan_v2(plan)
    _ross_v2_validate_state_shallow(plan, state, cancel_check)
    _ross_v2_replay_state(plan, state, cancel_check)
    cancel_check()
    return true
end

# `validate_state=false` is reserved for callers that produced the state in
# the same process or have already completed one authoritative forward replay.
# The public default preserves full standalone token validation.
function prepare_ro_sparse_index_batch_v2(
    plan::ROSparseROChannelPlanV2,
    state::ROSparseAdaptiveStateV2;
    cancel_check=() -> nothing,
    validate_state::Bool=true,
)
    cancel_check()
    if validate_state
        validate_ro_sparse_state_v2(plan, state; cancel_check=cancel_check)
    else
        _ross_v2_validate_state_shallow(plan, state, cancel_check)
    end
    schedule = _ross_v2_scheduler(plan, state, cancel_check)
    cancel_check()
    return schedule.batch
end

function _ross_v2_validate_index_batch_after_state(
    plan::ROSparseROChannelPlanV2,
    state::ROSparseAdaptiveStateV2,
    batch::ROSparseIndexBatchV2,
    ; cancel_check=() -> nothing,
)
    cancel_check()
    batch.schema_version == RO_SPARSE_RO_BATCH_VERSION ||
        throw(ArgumentError("unsupported v2 sparse batch version"))
    batch.plan_sha256 == plan.plan_sha256 || throw(ArgumentError(
        "v2 sparse batch belongs to a different plan"))
    batch.prior_state_sha256 == state.state_sha256 || throw(ArgumentError(
        "v2 sparse batch belongs to a different prior state"))
    dimension = length(plan.control_ids)
    length(batch.multi_index) == dimension &&
        all(level -> 1 <= level <= plan.limits.max_level,
            batch.multi_index) || throw(ArgumentError(
                "v2 sparse batch has an invalid multi-index"))
    length(batch.refinements_to_commit) <= plan.limits.max_multi_indices ||
        throw(ArgumentError("v2 sparse batch has too many refinements"))
    length(batch.pending_candidates_after) <= dimension ||
        throw(ArgumentError(
            "v2 sparse batch has too many pending candidates"))
    length(batch.requests) <= plan.limits.max_points || throw(ArgumentError(
        "v2 sparse batch exceeds the point budget"))
    0 <= batch.point_count <= plan.limits.max_points &&
        0 <= batch.payload_scalar_count <=
            plan.limits.max_payload_scalars &&
        0 <= batch.interpolation_work <= plan.limits.max_work ||
        throw(ArgumentError("v2 sparse batch cost exceeds its plan budget"))
    _ross_v2_all_cancel(request ->
            length(request.multi_index) == dimension &&
            length(request.node_ids) == dimension &&
            length(request.normalized_coordinates) == dimension &&
            length(request.control_coordinates) == dimension &&
            length(request.source_coordinates) ==
                length(plan.source_coordinate_ids),
        batch.requests, cancel_check) || throw(ArgumentError(
            "v2 sparse batch request dimensions are inconsistent"))
    batch.batch_sha256 == _ross_sha256(_ross_v2_batch_body(batch)) ||
        throw(ArgumentError("v2 sparse batch hash mismatch"))
    schedule = _ross_v2_scheduler(plan, state, cancel_check)
    schedule.batch === nothing && throw(ArgumentError(
        "v2 sparse batch was supplied for a terminal state"))
    JSON3.write(ro_sparse_index_batch_v2_payload(batch)) ==
        JSON3.write(ro_sparse_index_batch_v2_payload(schedule.batch)) ||
        throw(ArgumentError(
            "v2 sparse batch is not the canonical next transition"))
    cancel_check()
    return true
end

function validate_ro_sparse_index_batch_v2(
    plan::ROSparseROChannelPlanV2,
    state::ROSparseAdaptiveStateV2,
    batch::ROSparseIndexBatchV2,
    ; cancel_check=() -> nothing,
)
    cancel_check()
    validate_ro_sparse_state_v2(plan, state; cancel_check=cancel_check)
    return _ross_v2_validate_index_batch_after_state(
        plan, state, batch; cancel_check=cancel_check)
end

# `validate_prior_state=false` has the same trusted-chain precondition as the
# prepare keyword above; batch and next-state validation remain mandatory.
function commit_ro_sparse_index_batch_v2(
    plan::ROSparseROChannelPlanV2,
    state::ROSparseAdaptiveStateV2,
    batch::ROSparseIndexBatchV2,
    raw_evaluations;
    cancel_check=() -> nothing,
    validate_prior_state::Bool=true,
)
    cancel_check()
    if validate_prior_state
        validate_ro_sparse_state_v2(plan, state; cancel_check=cancel_check)
    else
        _ross_v2_validate_state_shallow(plan, state, cancel_check)
    end
    _ross_v2_validate_index_batch_after_state(
        plan, state, batch; cancel_check=cancel_check)
    evaluations = _ross_v2_normalize_evaluations(
        batch, raw_evaluations,
        length(plan.output_ids) * length(plan.control_ids), cancel_check)
    cancel_check()
    next_state = _ross_v2_commit_unchecked(
        plan, state, batch, evaluations, cancel_check)
    _ross_v2_validate_state_shallow(plan, next_state, cancel_check)
    cancel_check()
    return next_state
end

function _ross_v2_result_body(result::ROSparseSamplingResultV2)
    return (
        schema_version=result.schema_version,
        plan_sha256=result.plan_sha256,
        sampling_spec_sha256=result.sampling_spec_sha256,
        terminal_state_sha256=result.terminal_state_sha256,
        status=String(result.status),
        stopping_reason=String(result.stopping_reason),
        evidence_scope=String(result.evidence_scope),
        continuum_error_bound=nothing,
        accepted_multi_indices=Tuple(
            Tuple(index) for index in result.accepted_multi_indices),
        active_frontier=Tuple(
            Tuple(index) for index in result.active_frontier),
        refinement_order=Tuple(
            Tuple(index) for index in result.refinement_order),
        index_records=Tuple(_ross_v2_record_payload(record)
            for record in result.index_records),
        samples=Tuple(_ross_v2_sample_payload(sample)
            for sample in result.samples),
        unresolved_regions=Tuple(_ross_v2_unresolved_payload(region)
            for region in result.unresolved_regions),
        counters=(
            evaluated_point_count=result.evaluated_point_count,
            valid_point_count=result.valid_point_count,
            invalid_point_count=result.invalid_point_count,
            interpolation_work_consumed=
                result.interpolation_work_consumed,
            payload_scalar_count=result.payload_scalar_count,
            backend_work_unit_count=result.backend_work_unit_count,
        ),
        max_active_indicator=result.max_active_indicator,
    )
end

function ro_sparse_result_v2_payload(
    result::ROSparseSamplingResultV2;
    cancel_check=() -> nothing,
)
    payload = merge(
        _ross_v2_result_body(result), (result_sha256=result.result_sha256,))
    _ross_v2_assert_portable_token_size(payload, "result", cancel_check)
    return payload
end

function _ross_v2_new_result(
    plan,
    state,
    schedule,
    cancel_check=() -> nothing,
)
    cancel_check()
    refined = Set(schedule.refinements)
    accepted = Set(Tuple(index) for index in state.accepted_multi_indices)
    records_by_index = Dict(Tuple(record.multi_index) => record
        for record in state.index_records)
    active = sort!([index for index in accepted
        if !(index in refined) && records_by_index[index].indicator !== nothing])
    indicators = [records_by_index[index].indicator for index in active]
    valid_count = count(sample -> sample.evaluation.valid, state.samples)
    provisional = ROSparseSamplingResultV2(
        _ROSS_V2_TOKEN, RO_SPARSE_RO_RESULT_VERSION, plan.plan_sha256,
        plan.sampling_spec_sha256, state.state_sha256,
        schedule.status, schedule.reason, RO_SPARSE_EVIDENCE_SCOPE,
        state.accepted_multi_indices, [collect(index) for index in active],
        [collect(index) for index in schedule.refinements],
        state.index_records, state.samples, state.unresolved_regions,
        length(state.samples), valid_count,
        length(state.samples) - valid_count,
        state.interpolation_work_consumed, state.payload_scalar_count,
        state.backend_work_unit_count,
        isempty(indicators) ? nothing : maximum(indicators), "";
        cancel_check=cancel_check)
    cancel_check()
    result_hash = _ross_sha256(_ross_v2_result_body(provisional))
    cancel_check()
    result = ROSparseSamplingResultV2(
        _ROSS_V2_TOKEN, provisional.schema_version,
        provisional.plan_sha256, provisional.sampling_spec_sha256,
        provisional.terminal_state_sha256, provisional.status,
        provisional.stopping_reason, provisional.evidence_scope,
        provisional.accepted_multi_indices, provisional.active_frontier,
        provisional.refinement_order, provisional.index_records,
        provisional.samples, provisional.unresolved_regions,
        provisional.evaluated_point_count, provisional.valid_point_count,
        provisional.invalid_point_count,
        provisional.interpolation_work_consumed,
        provisional.payload_scalar_count,
        provisional.backend_work_unit_count,
        provisional.max_active_indicator, result_hash;
        cancel_check=cancel_check)
    _ross_v2_assert_portable_token_size(
        merge(_ross_v2_result_body(result),
            (result_sha256=result.result_sha256,)),
        "result", cancel_check)
    return result
end

"""
    validate_ro_sparse_result_v2(result; cancel_check)

Validate only the result token's local schema, self hash, and counters.  This
one-argument form does not prove that the token is the canonical finalization
of a terminal adaptive state.  Use the three-argument form with the bound plan
and terminal state for authoritative validation.
"""
function validate_ro_sparse_result_v2(
    result::ROSparseSamplingResultV2;
    cancel_check=() -> nothing,
)
    cancel_check()
    result.schema_version == RO_SPARSE_RO_RESULT_VERSION ||
        throw(ArgumentError("unsupported v2 sparse result version"))
    result.evidence_scope == RO_SPARSE_EVIDENCE_SCOPE ||
        throw(ArgumentError("v2 sparse result evidence scope changed"))
    length(result.samples) <= _ROSS_HARD_MAX_POINTS || throw(ArgumentError(
        "v2 sparse result exceeds the hard point bound"))
    length(result.index_records) <= _ROSS_HARD_MAX_MULTI_INDICES &&
        length(result.accepted_multi_indices) <=
            _ROSS_HARD_MAX_MULTI_INDICES &&
        length(result.active_frontier) <= _ROSS_HARD_MAX_MULTI_INDICES &&
        length(result.refinement_order) <= _ROSS_HARD_MAX_MULTI_INDICES &&
        length(result.unresolved_regions) <=
            _ROSS_HARD_MAX_MULTI_INDICES || throw(ArgumentError(
                "v2 sparse result exceeds the hard multi-index bound"))
    result.evaluated_point_count == length(result.samples) ||
        throw(ArgumentError("v2 sparse result point count mismatch"))
    result.evaluated_point_count >= 0 && result.valid_point_count >= 0 &&
        result.invalid_point_count >= 0 || throw(ArgumentError(
            "v2 sparse result counts must be nonnegative"))
    BigInt(result.valid_point_count) + BigInt(result.invalid_point_count) ==
        BigInt(result.evaluated_point_count) || throw(ArgumentError(
            "v2 sparse result validity counts do not add up"))
    valid_count = 0
    for (position, sample) in enumerate(result.samples)
        _ross_v2_cancel_checkpoint(cancel_check, position)
        sample.evaluation.valid && (valid_count += 1)
    end
    valid_count == result.valid_point_count || throw(ArgumentError(
        "v2 sparse result valid-point count does not match samples"))
    result.backend_work_unit_count == length(result.index_records) ||
        throw(ArgumentError("v2 result backend work-unit count mismatch"))
    result.interpolation_work_consumed >= 0 &&
        result.payload_scalar_count >= 0 &&
        result.backend_work_unit_count >= 0 || throw(ArgumentError(
            "v2 sparse result work counters must be nonnegative"))
    result.max_active_indicator === nothing ||
        isfinite(result.max_active_indicator) &&
            result.max_active_indicator >= 0.0 || throw(ArgumentError(
                "v2 sparse result active indicator must be finite and nonnegative"))
    cancel_check()
    result.result_sha256 == _ross_sha256(_ross_v2_result_body(result)) ||
        throw(ArgumentError("v2 sparse result hash mismatch"))
    cancel_check()
    return true
end

"""
    validate_ro_sparse_result_v2(plan, terminal_state, result; cancel_check)

Authoritatively validate a v2 result by replaying the supplied state by
default, requiring it to be terminal, recomputing its canonical finalization,
and comparing the complete portable result payload. `validate_state=false` is
reserved for a caller that already completed one authoritative forward replay.
"""
function validate_ro_sparse_result_v2(
    plan::ROSparseROChannelPlanV2,
    terminal_state::ROSparseAdaptiveStateV2,
    result::ROSparseSamplingResultV2;
    cancel_check=() -> nothing,
    validate_state::Bool=true,
)
    cancel_check()
    validate_ro_sparse_result_v2(result; cancel_check=cancel_check)
    expected = finalize_ro_sparse_state_v2(
        plan, terminal_state; cancel_check=cancel_check,
        validate_state=validate_state)
    cancel_check()
    JSON3.write(ro_sparse_result_v2_payload(result)) ==
        JSON3.write(ro_sparse_result_v2_payload(expected)) ||
        throw(ArgumentError(
            "v2 sparse result does not match terminal-state finalization"))
    cancel_check()
    return true
end

function finalize_ro_sparse_state_v2(
    plan::ROSparseROChannelPlanV2,
    state::ROSparseAdaptiveStateV2;
    cancel_check=() -> nothing,
    validate_state::Bool=true,
)
    cancel_check()
    if validate_state
        validate_ro_sparse_state_v2(plan, state; cancel_check=cancel_check)
    else
        _ross_v2_validate_state_shallow(plan, state, cancel_check)
    end
    schedule = _ross_v2_scheduler(plan, state, cancel_check)
    schedule.batch === nothing || throw(ArgumentError(
        "cannot finalize a v2 sparse state with a prepared next batch"))
    result = _ross_v2_new_result(plan, state, schedule, cancel_check)
    validate_ro_sparse_result_v2(result; cancel_check=cancel_check)
    cancel_check()
    return result
end

"Convenience runner over the same prepare/commit state transitions."
function adaptive_sparse_ro_field_v2(
    plan::ROSparseROChannelPlanV2,
    evaluator;
    cancel_check=() -> nothing,
    index_commit_callback=(_ -> nothing),
)
    evaluator isa Function || throw(ArgumentError(
        "evaluator must be a callback function"))
    index_commit_callback isa Function || throw(ArgumentError(
        "index_commit_callback must be a callback function"))
    state = initialize_ro_sparse_state_v2(plan; cancel_check=cancel_check)
    while true
        batch = prepare_ro_sparse_index_batch_v2(
            plan, state; cancel_check=cancel_check)
        batch === nothing && return finalize_ro_sparse_state_v2(
            plan, state; cancel_check=cancel_check)
        receipts = Any[]
        for request in batch.requests
            cancel_check()
            evaluation = evaluator(_ross_v2_copy_request(request))
            cancel_check()
            evaluation isa ROSparseEvaluation || throw(ArgumentError(
                "v2 evaluator must return ROSparseEvaluation"))
            push!(receipts,
                ro_sparse_ordered_evaluation_v2(request, evaluation))
        end
        next_state = commit_ro_sparse_index_batch_v2(
            plan, state, batch, receipts; cancel_check=cancel_check)
        cancel_check()
        index_commit_callback(ro_sparse_index_batch_v2_payload(batch))
        cancel_check()
        state = next_state
    end
end

# Strict portable-token decoders.  Restore never trusts a supplied self hash:
# it parses only the exact schema, rebuilds the canonical token, verifies the
# hash, and (for state/batch) replays or reconstructs the transition.

const _ROSS_V2_CANCEL_CHECK_STRIDE = 256
const _ROSS_V2_JSON_SCAN_CANCEL_STRIDE = 1024 * 1024

@inline function _ross_v2_cancel_checkpoint(cancel_check, position::Int)
    (position == 1 || position % _ROSS_V2_CANCEL_CHECK_STRIDE == 0) &&
        cancel_check()
    return nothing
end

function _ross_v2_preflight_json_depth(raw, name, cancel_check)
    depth = 0
    in_string = false
    escaped = false
    for (position, byte) in enumerate(codeunits(raw))
        (position == 1 ||
            position % _ROSS_V2_JSON_SCAN_CANCEL_STRIDE == 0) &&
            cancel_check()
        if in_string
            if escaped
                escaped = false
            elseif byte == UInt8('\\')
                escaped = true
            elseif byte == UInt8('"')
                in_string = false
            end
        elseif byte == UInt8('"')
            in_string = true
        elseif byte == UInt8('{') || byte == UInt8('[')
            depth += 1
            depth <= _ROSS_V2_MAX_PORTABLE_TOKEN_DEPTH ||
                throw(ArgumentError(
                    "$name exceeds the portable-token nesting limit of " *
                    "$(_ROSS_V2_MAX_PORTABLE_TOKEN_DEPTH)"))
        elseif byte == UInt8('}') || byte == UInt8(']')
            depth -= 1
            depth >= 0 || throw(ArgumentError(
                "$name has unbalanced JSON nesting"))
        end
    end
    cancel_check()
    return nothing
end

function _ross_v2_decode(
    raw,
    name;
    cancel_check=() -> nothing,
)
    cancel_check()
    if raw isa AbstractString
        byte_count = ncodeunits(raw)
        byte_count <= _ROSS_V2_MAX_PORTABLE_TOKEN_BYTES ||
            throw(ArgumentError(
                "$name exceeds the portable-token byte limit of " *
                "$(_ROSS_V2_MAX_PORTABLE_TOKEN_BYTES) bytes"))
        _ross_v2_preflight_json_depth(raw, name, cancel_check)
        cancel_check()
        payload = JSON3.read(raw)
        cancel_check()
        return payload
    end
    return raw
end

function _ross_v2_validate_source_encoding(
    raw,
    canonical_payload,
    name,
    cancel_check,
)
    raw isa AbstractString || return true
    cancel_check()
    canonical = JSON3.write(canonical_payload)
    cancel_check()
    raw == canonical || throw(ArgumentError(
        "$name must use canonical JSON3 encoding"))
    return true
end

function _ross_v2_object_keys(raw)
    raw isa NamedTuple && return Set(String.(propertynames(raw)))
    try
        return Set(String(key) for key in keys(raw))
    catch
        throw(ArgumentError("portable v2 token sections must be objects"))
    end
end

function _ross_v2_require_keys(raw, expected, name)
    _ross_v2_object_keys(raw) == Set(String.(expected)) ||
        throw(ArgumentError("$name fields are not canonical"))
    return raw
end

function _ross_v2_get(raw, key::Symbol)
    raw isa NamedTuple && return getproperty(raw, key)
    raw isa AbstractDict && haskey(raw, key) && return raw[key]
    raw isa AbstractDict && haskey(raw, String(key)) && return raw[String(key)]
    hasproperty(raw, key) && return getproperty(raw, key)
    try
        return raw[key]
    catch
        try
            return raw[String(key)]
        catch
            throw(ArgumentError("portable v2 token is missing $(key)"))
        end
    end
end

function _ross_v2_collection(
    raw,
    name;
    max_length::Union{Nothing,Int}=nothing,
    exact_length::Union{Nothing,Int}=nothing,
    cancel_check=() -> nothing,
)
    cancel_check()
    (raw isa AbstractVector || raw isa Tuple) || throw(ArgumentError(
        "$name must be an ordered collection"))
    count = length(raw)
    exact_length === nothing || count == exact_length || throw(ArgumentError(
        "$name must contain exactly $exact_length items"))
    max_length === nothing || count <= max_length || throw(ArgumentError(
        "$name must not exceed $max_length items"))
    values = collect(raw)
    cancel_check()
    return values
end

function _ross_v2_string(raw, name; maximum=256)
    raw isa AbstractString || throw(ArgumentError("$name must be a string"))
    value = String(raw)
    isempty(value) && throw(ArgumentError("$name must not be empty"))
    ncodeunits(value) <= maximum || throw(ArgumentError(
        "$name must not exceed $maximum UTF-8 bytes"))
    return value
end

function _ross_v2_int(raw, name)
    raw isa Integer && !(raw isa Bool) || throw(ArgumentError(
        "$name must be an integer"))
    return try
        Int(raw)
    catch
        throw(ArgumentError("$name must fit in Int"))
    end
end

function _ross_v2_float(raw, name)
    raw isa Real && !(raw isa Bool) || throw(ArgumentError(
        "$name must be a real number"))
    value = Float64(raw)
    isfinite(value) || throw(ArgumentError("$name must be finite"))
    return value == 0.0 ? 0.0 : value
end

function _ross_v2_int_vector(
    raw,
    name;
    max_length::Union{Nothing,Int}=nothing,
    exact_length::Union{Nothing,Int}=nothing,
    cancel_check=() -> nothing,
)
    raw_values = _ross_v2_collection(raw, name;
        max_length=max_length, exact_length=exact_length,
        cancel_check=cancel_check)
    values = Int[]
    sizehint!(values, length(raw_values))
    for (position, value) in enumerate(raw_values)
        _ross_v2_cancel_checkpoint(cancel_check, position)
        push!(values, _ross_v2_int(value, name))
    end
    cancel_check()
    return values
end

function _ross_v2_float_vector(
    raw,
    name;
    max_length::Union{Nothing,Int}=nothing,
    exact_length::Union{Nothing,Int}=nothing,
    cancel_check=() -> nothing,
)
    raw_values = _ross_v2_collection(raw, name;
        max_length=max_length, exact_length=exact_length,
        cancel_check=cancel_check)
    values = Float64[]
    sizehint!(values, length(raw_values))
    for (position, value) in enumerate(raw_values)
        _ross_v2_cancel_checkpoint(cancel_check, position)
        push!(values, _ross_v2_float(value, name))
    end
    cancel_check()
    return values
end

function _ross_v2_string_vector(
    raw,
    name;
    max_length::Union{Nothing,Int}=nothing,
    exact_length::Union{Nothing,Int}=nothing,
    cancel_check=() -> nothing,
)
    raw_values = _ross_v2_collection(raw, name;
        max_length=max_length, exact_length=exact_length,
        cancel_check=cancel_check)
    values = String[]
    sizehint!(values, length(raw_values))
    for (position, value) in enumerate(raw_values)
        _ross_v2_cancel_checkpoint(cancel_check, position)
        push!(values, _ross_v2_string(value, name; maximum=256))
    end
    cancel_check()
    return values
end

function _ross_v2_hash(raw, name)
    value = _ross_v2_string(raw, name; maximum=64)
    occursin(r"^[0-9a-f]{64}$", value) || throw(ArgumentError(
        "$name must be a lowercase SHA-256 hex digest"))
    return value
end

function _ross_v2_parse_request(
    raw;
    dimension::Union{Nothing,Int}=nothing,
    source_count::Union{Nothing,Int}=nothing,
    cancel_check=() -> nothing,
)
    cancel_check()
    _ross_v2_require_keys(raw, (
        :point_id, :multi_index, :node_ids, :normalized_coordinates,
        :control_coordinates, :source_coordinates), "point request")
    dimension_limit = dimension === nothing ? nothing : dimension
    return ROSparsePointRequest(
        _ross_v2_string(_ross_v2_get(raw, :point_id), "point_id";
            maximum=512),
        _ross_v2_int_vector(
            _ross_v2_get(raw, :multi_index), "multi_index";
            exact_length=dimension_limit, cancel_check=cancel_check),
        _ross_v2_string_vector(
            _ross_v2_get(raw, :node_ids), "node_ids";
            exact_length=dimension_limit, cancel_check=cancel_check),
        _ross_v2_float_vector(_ross_v2_get(raw,
            :normalized_coordinates), "normalized_coordinates";
            exact_length=dimension_limit, cancel_check=cancel_check),
        _ross_v2_float_vector(_ross_v2_get(raw,
            :control_coordinates), "control_coordinates";
            exact_length=dimension_limit, cancel_check=cancel_check),
        _ross_v2_float_vector(_ross_v2_get(raw,
            :source_coordinates), "source_coordinates";
            exact_length=source_count, cancel_check=cancel_check))
end

function restore_ro_sparse_ro_channel_plan_v2(
    raw;
    cancel_check=() -> nothing,
)
    payload = _ross_v2_decode(
        raw, "v2 RO-channel plan"; cancel_check=cancel_check)
    _ross_v2_require_keys(payload, (
        :schema_version, :sampling_spec, :execution_budget,
        :portable_token_policy,
        :sampling_spec_sha256, :plan_sha256), "v2 RO-channel plan")
    _ross_v2_string(_ross_v2_get(payload, :schema_version),
        "schema_version") == RO_SPARSE_RO_CHANNEL_PLAN_VERSION ||
        throw(ArgumentError("unsupported v2 RO-channel plan version"))
    spec = _ross_v2_get(payload, :sampling_spec)
    _ross_v2_require_keys(spec, (
        :schema_version, :chart, :domain, :outputs, :fixed_background,
        :policy), "v2 sampling spec")
    _ross_v2_string(_ross_v2_get(spec, :schema_version),
        "sampling spec schema_version") ==
        RO_SPARSE_RO_CHANNEL_PLAN_VERSION || throw(ArgumentError(
            "sampling spec and plan schema versions differ"))
    chart = _ross_v2_get(spec, :chart)
    _ross_v2_require_keys(chart, (
        :chart_id, :control_ids, :source_coordinate_ids, :jacobian),
        "v2 sampling chart")
    chart_id = _ross_v2_string(
        _ross_v2_get(chart, :chart_id), "chart_id"; maximum=128)
    control_ids = _ross_v2_string_vector(
        _ross_v2_get(chart, :control_ids), "control_ids";
        max_length=_ROSS_HARD_MAX_DIMENSIONS, cancel_check=cancel_check)
    source_ids = _ross_v2_string_vector(
        _ross_v2_get(chart, :source_coordinate_ids),
        "source_coordinate_ids"; max_length=1_024,
        cancel_check=cancel_check)
    rows = _ross_v2_collection(
        _ross_v2_get(chart, :jacobian), "chart jacobian";
        exact_length=length(source_ids), cancel_check=cancel_check)
    jacobian = Matrix{Float64}(undef, length(rows), length(control_ids))
    for (row_position, raw_row) in enumerate(rows)
        _ross_v2_cancel_checkpoint(cancel_check, row_position)
        row = _ross_v2_float_vector(
            raw_row, "chart jacobian row";
            exact_length=length(control_ids), cancel_check=cancel_check)
        jacobian[row_position, :] = row
    end
    domain = _ross_v2_get(spec, :domain)
    _ross_v2_require_keys(domain, (:lower, :upper), "v2 sampling domain")
    lower = _ross_v2_float_vector(
        _ross_v2_get(domain, :lower), "domain lower";
        exact_length=length(control_ids), cancel_check=cancel_check)
    upper = _ross_v2_float_vector(
        _ross_v2_get(domain, :upper), "domain upper";
        exact_length=length(control_ids), cancel_check=cancel_check)
    outputs = _ross_v2_get(spec, :outputs)
    _ross_v2_require_keys(outputs,
        (:output_ids, :reaction_order_components), "v2 sampling outputs")
    output_ids = _ross_v2_string_vector(
        _ross_v2_get(outputs, :output_ids), "output_ids";
        max_length=_ROSS_HARD_MAX_OUTPUTS, cancel_check=cancel_check)
    components = _ross_v2_collection(
        _ross_v2_get(outputs, :reaction_order_components),
        "reaction_order_components"; max_length=_ROSS_HARD_MAX_OUTPUTS,
        cancel_check=cancel_check)
    expected_components = _ross_v2_component_payload(output_ids, control_ids)
    length(components) == length(expected_components) || throw(ArgumentError(
        "RO component list must cover output x input exactly"))
    for (position, (raw_component, expected)) in
            enumerate(zip(components, expected_components))
        _ross_v2_cancel_checkpoint(cancel_check, position)
        _ross_v2_require_keys(raw_component,
            (:channel_index, :output_id, :input_axis_id), "RO component")
        _ross_v2_int(_ross_v2_get(raw_component, :channel_index),
            "channel_index") == expected.channel_index ||
            throw(ArgumentError("RO component channel order is not canonical"))
        _ross_v2_string(_ross_v2_get(raw_component, :output_id),
            "component output_id") == expected.output_id ||
            throw(ArgumentError("RO component output binding is not canonical"))
        _ross_v2_string(_ross_v2_get(raw_component, :input_axis_id),
            "component input_axis_id") == expected.input_axis_id ||
            throw(ArgumentError("RO component input binding is not canonical"))
    end
    background = _ross_v2_float_vector(
        _ross_v2_get(spec, :fixed_background), "fixed_background";
        exact_length=length(source_ids), cancel_check=cancel_check)
    policy = _ross_v2_get(spec, :policy)
    _ross_v2_require_keys(policy, (
        :initial_total_degree, :indicator, :channel_order, :frontier_order,
        :invalid_policy, :surplus_tolerance, :evidence_scope,
        :continuum_error_bound), "v2 sampling policy")
    _ross_v2_string(_ross_v2_get(policy, :indicator), "indicator") ==
        "ordered_ro_components_linf_hierarchical_surplus" ||
        throw(ArgumentError("unsupported v2 indicator policy"))
    _ross_v2_string(_ross_v2_get(policy, :channel_order), "channel_order") ==
        "output_major_then_input_minor" || throw(ArgumentError(
            "unsupported v2 channel order"))
    _ross_v2_string(_ross_v2_get(policy, :frontier_order), "frontier_order") ==
        "indicator_desc_total_level_asc_lexicographic" ||
        throw(ArgumentError("unsupported v2 frontier order"))
    _ross_v2_string(_ross_v2_get(policy, :invalid_policy), "invalid_policy") ==
        "unresolved_cone_blocks_descendants_only" ||
        throw(ArgumentError("unsupported v2 invalid policy"))
    _ross_v2_string(_ross_v2_get(policy, :evidence_scope), "evidence_scope") ==
        String(RO_SPARSE_EVIDENCE_SCOPE) || throw(ArgumentError(
            "unsupported v2 evidence scope"))
    _ross_v2_get(policy, :continuum_error_bound) === nothing ||
        throw(ArgumentError("v2 cannot claim a continuum error bound"))
    portable_policy = _ross_v2_get(payload, :portable_token_policy)
    _ross_v2_require_keys(portable_policy,
        (:max_token_bytes, :enforcement), "v2 portable-token policy")
    _ross_v2_int(
        _ross_v2_get(portable_policy, :max_token_bytes),
        "max_token_bytes") == _ROSS_V2_MAX_PORTABLE_TOKEN_BYTES ||
        throw(ArgumentError("unsupported v2 portable-token byte budget"))
    _ross_v2_string(
        _ross_v2_get(portable_policy, :enforcement),
        "portable-token enforcement") ==
        "all_v2_token_construction_publication_and_restore" ||
        throw(ArgumentError("unsupported v2 portable-token enforcement"))
    budget = _ross_v2_get(payload, :execution_budget)
    _ross_v2_require_keys(budget, (
        :max_level, :max_points, :max_multi_indices,
        :max_interpolation_work, :max_payload_scalars),
        "v2 execution budget")
    limits = ROSparseSamplingLimits(
        max_level=_ross_v2_int(_ross_v2_get(budget, :max_level),
            "max_level"),
        max_points=_ross_v2_int(_ross_v2_get(budget, :max_points),
            "max_points"),
        max_multi_indices=_ross_v2_int(
            _ross_v2_get(budget, :max_multi_indices), "max_multi_indices"),
        max_work=_ross_v2_int(
            _ross_v2_get(budget, :max_interpolation_work),
            "max_interpolation_work"),
        max_payload_scalars=_ross_v2_int(
            _ross_v2_get(budget, :max_payload_scalars),
            "max_payload_scalars"))
    plan = ROSparseROChannelPlanV2(
        chart_id=chart_id,
        control_ids=control_ids,
        source_coordinate_ids=source_ids,
        chart_jacobian=jacobian,
        domain_lower=lower,
        domain_upper=upper,
        output_ids=output_ids,
        fixed_background=background,
        surplus_tolerance=_ross_v2_float(
            _ross_v2_get(policy, :surplus_tolerance), "surplus_tolerance"),
        initial_total_degree=_ross_v2_int(
            _ross_v2_get(policy, :initial_total_degree),
            "initial_total_degree"),
        limits=limits)
    supplied_spec_hash = _ross_v2_hash(
        _ross_v2_get(payload, :sampling_spec_sha256),
        "sampling_spec_sha256")
    supplied_plan_hash = _ross_v2_hash(
        _ross_v2_get(payload, :plan_sha256), "plan_sha256")
    supplied_spec_hash == plan.sampling_spec_sha256 || throw(ArgumentError(
        "restored v2 sampling-spec hash mismatch"))
    supplied_plan_hash == plan.plan_sha256 || throw(ArgumentError(
        "restored v2 plan hash mismatch"))
    _ross_v2_validate_source_encoding(
        raw,
        merge(_ross_v2_plan_body(plan), (plan_sha256=plan.plan_sha256,)),
        "v2 RO-channel plan", cancel_check)
    cancel_check()
    return plan
end

function _ross_v2_parse_record(
    raw;
    dimension::Union{Nothing,Int}=nothing,
    max_points::Union{Nothing,Int}=nothing,
    cancel_check=() -> nothing,
)
    cancel_check()
    _ross_v2_require_keys(raw,
        (:multi_index, :point_ids, :status, :indicator), "index record")
    status_text = _ross_v2_string(
        _ross_v2_get(raw, :status), "record status")
    status_text in ("resolved", "unresolved_gap") || throw(ArgumentError(
        "invalid v2 sparse record status"))
    indicator_raw = _ross_v2_get(raw, :indicator)
    indicator = indicator_raw === nothing ? nothing :
        _ross_v2_float(indicator_raw, "record indicator")
    return ROSparseIndexRecord(
        _ross_v2_int_vector(
            _ross_v2_get(raw, :multi_index), "record multi_index";
            exact_length=dimension, cancel_check=cancel_check),
        _ross_v2_string_vector(
            _ross_v2_get(raw, :point_ids), "record point_ids";
            max_length=max_points, cancel_check=cancel_check),
        Symbol(status_text), indicator)
end

function _ross_v2_parse_sample(
    raw;
    dimension::Union{Nothing,Int}=nothing,
    source_count::Union{Nothing,Int}=nothing,
    output_count::Union{Nothing,Int}=nothing,
    cancel_check=() -> nothing,
)
    cancel_check()
    _ross_v2_require_keys(raw, (
        :point_id, :multi_index, :node_ids, :normalized_coordinates,
        :control_coordinates, :source_coordinates, :valid, :values,
        :invalid_reason, :surplus), "sample")
    request = _ross_v2_parse_request((
        point_id=_ross_v2_get(raw, :point_id),
        multi_index=_ross_v2_get(raw, :multi_index),
        node_ids=_ross_v2_get(raw, :node_ids),
        normalized_coordinates=_ross_v2_get(raw, :normalized_coordinates),
        control_coordinates=_ross_v2_get(raw, :control_coordinates),
        source_coordinates=_ross_v2_get(raw, :source_coordinates));
        dimension=dimension, source_count=source_count,
        cancel_check=cancel_check)
    valid_raw = _ross_v2_get(raw, :valid)
    valid_raw isa Bool || throw(ArgumentError("sample valid must be Boolean"))
    values_raw = _ross_v2_get(raw, :values)
    reason_raw = _ross_v2_get(raw, :invalid_reason)
    evaluation = if valid_raw
        reason_raw === nothing || throw(ArgumentError(
            "valid sample cannot carry invalid_reason"))
        values_raw === nothing && throw(ArgumentError(
            "valid sample requires values"))
        ROSparseEvaluation(true,
            _ross_v2_float_vector(
                values_raw, "sample values"; exact_length=output_count,
                cancel_check=cancel_check), nothing)
    else
        values_raw === nothing || throw(ArgumentError(
            "invalid sample cannot carry values"))
        reason = _ross_v2_string(reason_raw, "sample invalid_reason";
            maximum=128)
        ROSparseEvaluation(false, nothing, Symbol(reason))
    end
    surplus_raw = _ross_v2_get(raw, :surplus)
    surplus = surplus_raw === nothing ? nothing :
        _ross_v2_float_vector(
            surplus_raw, "sample surplus"; exact_length=output_count,
            cancel_check=cancel_check)
    return ROSparseSample(request, evaluation, surplus)
end

function _ross_v2_parse_unresolved(
    raw;
    dimension::Union{Nothing,Int}=nothing,
    max_points::Union{Nothing,Int}=nothing,
    cancel_check=() -> nothing,
)
    cancel_check()
    _ross_v2_require_keys(raw,
        (:multi_index, :point_ids, :reasons), "unresolved region")
    raw_reasons = _ross_v2_collection(
        _ross_v2_get(raw, :reasons), "unresolved reasons";
        max_length=max_points, cancel_check=cancel_check)
    reasons = Symbol[]
    sizehint!(reasons, length(raw_reasons))
    for (position, reason) in enumerate(raw_reasons)
        _ross_v2_cancel_checkpoint(cancel_check, position)
        push!(reasons, Symbol(_ross_v2_string(
            reason, "unresolved reason"; maximum=128)))
    end
    return ROSparseUnresolvedRegion(
        _ross_v2_int_vector(
            _ross_v2_get(raw, :multi_index), "unresolved multi_index";
            exact_length=dimension, cancel_check=cancel_check),
        _ross_v2_string_vector(
            _ross_v2_get(raw, :point_ids), "unresolved point_ids";
            max_length=max_points, cancel_check=cancel_check),
        reasons)
end

function restore_ro_sparse_state_v2(
    plan::ROSparseROChannelPlanV2,
    raw;
    cancel_check=() -> nothing,
)
    cancel_check()
    validate_ro_sparse_ro_channel_plan_v2(plan)
    cancel_check()
    payload = _ross_v2_decode(
        raw, "v2 sparse state"; cancel_check=cancel_check)
    _ross_v2_require_keys(payload, (
        :schema_version, :plan_sha256, :sampling_spec_sha256,
        :initial_cursor, :accepted_multi_indices, :refinement_order,
        :pending_candidates, :index_records, :samples,
        :unresolved_regions, :counters, :state_sha256), "v2 sparse state")
    _ross_v2_string(_ross_v2_get(payload, :schema_version),
        "state schema_version") == RO_SPARSE_RO_STATE_VERSION ||
        throw(ArgumentError("unsupported v2 sparse state version"))
    _ross_v2_hash(_ross_v2_get(payload, :plan_sha256),
        "state plan_sha256") == plan.plan_sha256 || throw(ArgumentError(
            "restored state belongs to a different plan"))
    _ross_v2_hash(_ross_v2_get(payload, :sampling_spec_sha256),
        "state sampling_spec_sha256") == plan.sampling_spec_sha256 ||
        throw(ArgumentError(
            "restored state sampling identity differs from the plan"))
    dimension = length(plan.control_ids)
    source_count = length(plan.source_coordinate_ids)
    output_count = length(plan.output_ids) * dimension
    function parse_indices(raw_indices, name, max_length)
        raw_items = _ross_v2_collection(raw_indices, name;
            max_length=max_length, cancel_check=cancel_check)
        indices = Vector{Vector{Int}}()
        sizehint!(indices, length(raw_items))
        for (position, item) in enumerate(raw_items)
            _ross_v2_cancel_checkpoint(cancel_check, position)
            index = _ross_v2_int_vector(item, name;
                exact_length=dimension, cancel_check=cancel_check)
            all(level -> 1 <= level <= plan.limits.max_level, index) ||
                throw(ArgumentError(
                    "$name levels must lie within the declared budget"))
            push!(indices, index)
        end
        cancel_check()
        return indices
    end
    accepted = parse_indices(
        _ross_v2_get(payload, :accepted_multi_indices),
        "accepted multi-index", plan.limits.max_multi_indices)
    refinements = parse_indices(
        _ross_v2_get(payload, :refinement_order),
        "refinement multi-index", plan.limits.max_multi_indices)
    pending = parse_indices(
        _ross_v2_get(payload, :pending_candidates),
        "pending multi-index", dimension)
    raw_records = _ross_v2_collection(
        _ross_v2_get(payload, :index_records), "index_records";
        max_length=plan.limits.max_multi_indices,
        cancel_check=cancel_check)
    records = ROSparseIndexRecord[]
    sizehint!(records, length(raw_records))
    for (position, item) in enumerate(raw_records)
        _ross_v2_cancel_checkpoint(cancel_check, position)
        push!(records, _ross_v2_parse_record(item;
            dimension=dimension, max_points=plan.limits.max_points,
            cancel_check=cancel_check))
    end
    raw_samples = _ross_v2_collection(
        _ross_v2_get(payload, :samples), "samples";
        max_length=plan.limits.max_points, cancel_check=cancel_check)
    samples = ROSparseSample[]
    sizehint!(samples, length(raw_samples))
    for (position, item) in enumerate(raw_samples)
        _ross_v2_cancel_checkpoint(cancel_check, position)
        push!(samples, _ross_v2_parse_sample(item;
            dimension=dimension, source_count=source_count,
            output_count=output_count, cancel_check=cancel_check))
    end
    raw_unresolved = _ross_v2_collection(
        _ross_v2_get(payload, :unresolved_regions), "unresolved_regions";
        max_length=plan.limits.max_multi_indices,
        cancel_check=cancel_check)
    unresolved = ROSparseUnresolvedRegion[]
    sizehint!(unresolved, length(raw_unresolved))
    for (position, item) in enumerate(raw_unresolved)
        _ross_v2_cancel_checkpoint(cancel_check, position)
        push!(unresolved, _ross_v2_parse_unresolved(item;
            dimension=dimension, max_points=plan.limits.max_points,
            cancel_check=cancel_check))
    end
    counters = _ross_v2_get(payload, :counters)
    _ross_v2_require_keys(counters, (
        :interpolation_work_consumed, :payload_scalar_count,
        :backend_work_unit_count), "v2 state counters")
    state = _ross_v2_new_state(
        plan,
        initial_cursor=_ross_v2_int(
            _ross_v2_get(payload, :initial_cursor), "initial_cursor"),
        accepted_multi_indices=accepted,
        refinement_order=refinements,
        pending_candidates=pending,
        index_records=records,
        samples=samples,
        unresolved_regions=unresolved,
        interpolation_work_consumed=_ross_v2_int(
            _ross_v2_get(counters, :interpolation_work_consumed),
            "interpolation_work_consumed"),
        payload_scalar_count=_ross_v2_int(
            _ross_v2_get(counters, :payload_scalar_count),
            "payload_scalar_count"),
        backend_work_unit_count=_ross_v2_int(
            _ross_v2_get(counters, :backend_work_unit_count),
            "backend_work_unit_count"),
        cancel_check=cancel_check)
    supplied_hash = _ross_v2_hash(
        _ross_v2_get(payload, :state_sha256), "state_sha256")
    supplied_hash == state.state_sha256 || throw(ArgumentError(
        "restored v2 sparse state hash mismatch"))
    validate_ro_sparse_state_v2(plan, state; cancel_check=cancel_check)
    _ross_v2_validate_source_encoding(
        raw,
        merge(_ross_v2_state_body(state),
            (state_sha256=state.state_sha256,)),
        "v2 sparse state", cancel_check)
    cancel_check()
    return state
end

function restore_ro_sparse_index_batch_v2(
    plan::ROSparseROChannelPlanV2,
    state::ROSparseAdaptiveStateV2,
    raw,
    ; cancel_check=() -> nothing,
    validate_prior_state::Bool=true,
)
    cancel_check()
    if validate_prior_state
        validate_ro_sparse_state_v2(plan, state; cancel_check=cancel_check)
    else
        _ross_v2_validate_state_shallow(plan, state, cancel_check)
    end
    payload = _ross_v2_decode(
        raw, "v2 sparse index batch"; cancel_check=cancel_check)
    _ross_v2_require_keys(payload, (
        :schema_version, :plan_sha256, :prior_state_sha256, :batch_ordinal,
        :multi_index, :refinements_to_commit, :initial_cursor_after,
        :pending_candidates_after, :requests, :cost, :batch_sha256),
        "v2 sparse index batch")
    _ross_v2_string(_ross_v2_get(payload, :schema_version),
        "batch schema_version") == RO_SPARSE_RO_BATCH_VERSION ||
        throw(ArgumentError("unsupported v2 sparse batch version"))
    dimension = length(plan.control_ids)
    source_count = length(plan.source_coordinate_ids)
    function parse_indices(raw_indices, name, max_length)
        raw_items = _ross_v2_collection(raw_indices, name;
            max_length=max_length, cancel_check=cancel_check)
        indices = Vector{Vector{Int}}()
        sizehint!(indices, length(raw_items))
        for (position, item) in enumerate(raw_items)
            _ross_v2_cancel_checkpoint(cancel_check, position)
            index = _ross_v2_int_vector(item, name;
                exact_length=dimension, cancel_check=cancel_check)
            all(level -> 1 <= level <= plan.limits.max_level, index) ||
                throw(ArgumentError(
                    "$name levels must lie within the declared budget"))
            push!(indices, index)
        end
        cancel_check()
        return indices
    end
    multi_index = only(parse_indices(
        (_ross_v2_get(payload, :multi_index),),
        "batch multi_index", 1))
    refinements = parse_indices(
        _ross_v2_get(payload, :refinements_to_commit),
        "batch refinement", plan.limits.max_multi_indices)
    pending = parse_indices(
        _ross_v2_get(payload, :pending_candidates_after),
        "pending candidate", dimension)
    raw_requests = _ross_v2_collection(
        _ross_v2_get(payload, :requests), "batch requests";
        max_length=plan.limits.max_points, cancel_check=cancel_check)
    requests = ROSparsePointRequest[]
    sizehint!(requests, length(raw_requests))
    for (position, item) in enumerate(raw_requests)
        _ross_v2_cancel_checkpoint(cancel_check, position)
        push!(requests, _ross_v2_parse_request(item;
            dimension=dimension, source_count=source_count,
            cancel_check=cancel_check))
    end
    cost = _ross_v2_get(payload, :cost)
    _ross_v2_require_keys(cost, (
        :point_count, :payload_scalar_count, :interpolation_work,
        :backend_work_units), "v2 batch cost")
    _ross_v2_int(_ross_v2_get(cost, :backend_work_units),
        "backend_work_units") == 1 || throw(ArgumentError(
            "one v2 index batch must equal one backend work unit"))
    point_count = _ross_v2_int(
        _ross_v2_get(cost, :point_count), "point_count")
    payload_count = _ross_v2_int(
        _ross_v2_get(cost, :payload_scalar_count), "payload_scalar_count")
    interpolation_work = _ross_v2_int(
        _ross_v2_get(cost, :interpolation_work), "interpolation_work")
    0 <= point_count <= plan.limits.max_points || throw(ArgumentError(
        "batch point_count exceeds the declared budget"))
    0 <= payload_count <= plan.limits.max_payload_scalars ||
        throw(ArgumentError(
            "batch payload_scalar_count exceeds the declared budget"))
    0 <= interpolation_work <= plan.limits.max_work ||
        throw(ArgumentError(
            "batch interpolation_work exceeds the declared budget"))
    batch = ROSparseIndexBatchV2(
        _ROSS_V2_TOKEN, RO_SPARSE_RO_BATCH_VERSION,
        _ross_v2_hash(_ross_v2_get(payload, :plan_sha256),
            "batch plan_sha256"),
        _ross_v2_hash(_ross_v2_get(payload, :prior_state_sha256),
            "prior_state_sha256"),
        _ross_v2_int(_ross_v2_get(payload, :batch_ordinal), "batch_ordinal"),
        multi_index,
        refinements,
        _ross_v2_int(_ross_v2_get(payload, :initial_cursor_after),
            "initial_cursor_after"),
        pending,
        requests,
        point_count,
        payload_count,
        interpolation_work,
        _ross_v2_hash(_ross_v2_get(payload, :batch_sha256),
            "batch_sha256"); cancel_check=cancel_check)
    _ross_v2_validate_index_batch_after_state(
        plan, state, batch; cancel_check=cancel_check)
    _ross_v2_validate_source_encoding(
        raw,
        merge(_ross_v2_batch_body(batch),
            (batch_sha256=batch.batch_sha256,)),
        "v2 sparse index batch", cancel_check)
    cancel_check()
    return batch
end

function restore_ro_sparse_result_v2(
    plan::ROSparseROChannelPlanV2,
    terminal_state::ROSparseAdaptiveStateV2,
    raw;
    cancel_check=() -> nothing,
    validate_terminal_state::Bool=true,
)
    cancel_check()
    if validate_terminal_state
        validate_ro_sparse_state_v2(
            plan, terminal_state; cancel_check=cancel_check)
    else
        _ross_v2_validate_state_shallow(
            plan, terminal_state, cancel_check)
    end
    payload = _ross_v2_decode(
        raw, "v2 sparse result"; cancel_check=cancel_check)
    _ross_v2_require_keys(payload, (
        :schema_version, :plan_sha256, :sampling_spec_sha256,
        :terminal_state_sha256, :status, :stopping_reason, :evidence_scope,
        :continuum_error_bound, :accepted_multi_indices, :active_frontier,
        :refinement_order, :index_records, :samples, :unresolved_regions,
        :counters, :max_active_indicator, :result_sha256),
        "v2 sparse result")
    _ross_v2_string(_ross_v2_get(payload, :schema_version),
        "result schema_version") == RO_SPARSE_RO_RESULT_VERSION ||
        throw(ArgumentError("unsupported v2 sparse result version"))
    _ross_v2_get(payload, :continuum_error_bound) === nothing ||
        throw(ArgumentError("v2 sparse results cannot claim a continuum bound"))
    dimension = length(plan.control_ids)
    source_count = length(plan.source_coordinate_ids)
    output_count = length(plan.output_ids) * dimension
    function parse_indices(raw_indices, name, max_length)
        raw_items = _ross_v2_collection(raw_indices, name;
            max_length=max_length, cancel_check=cancel_check)
        indices = Vector{Vector{Int}}()
        sizehint!(indices, length(raw_items))
        for (position, item) in enumerate(raw_items)
            _ross_v2_cancel_checkpoint(cancel_check, position)
            index = _ross_v2_int_vector(item, name;
                exact_length=dimension, cancel_check=cancel_check)
            all(level -> 1 <= level <= plan.limits.max_level, index) ||
                throw(ArgumentError(
                    "$name levels must lie within the declared budget"))
            push!(indices, index)
        end
        cancel_check()
        return indices
    end
    accepted = parse_indices(
        _ross_v2_get(payload, :accepted_multi_indices),
        "result accepted multi-index", plan.limits.max_multi_indices)
    active = parse_indices(
        _ross_v2_get(payload, :active_frontier),
        "result active-frontier multi-index", plan.limits.max_multi_indices)
    refinements = parse_indices(
        _ross_v2_get(payload, :refinement_order),
        "result refinement multi-index", plan.limits.max_multi_indices)
    raw_records = _ross_v2_collection(
        _ross_v2_get(payload, :index_records), "result index_records";
        max_length=plan.limits.max_multi_indices,
        cancel_check=cancel_check)
    records = ROSparseIndexRecord[]
    sizehint!(records, length(raw_records))
    for (position, item) in enumerate(raw_records)
        _ross_v2_cancel_checkpoint(cancel_check, position)
        push!(records, _ross_v2_parse_record(item;
            dimension=dimension, max_points=plan.limits.max_points,
            cancel_check=cancel_check))
    end
    raw_samples = _ross_v2_collection(
        _ross_v2_get(payload, :samples), "result samples";
        max_length=plan.limits.max_points, cancel_check=cancel_check)
    samples = ROSparseSample[]
    sizehint!(samples, length(raw_samples))
    for (position, item) in enumerate(raw_samples)
        _ross_v2_cancel_checkpoint(cancel_check, position)
        push!(samples, _ross_v2_parse_sample(item;
            dimension=dimension, source_count=source_count,
            output_count=output_count, cancel_check=cancel_check))
    end
    raw_unresolved = _ross_v2_collection(
        _ross_v2_get(payload, :unresolved_regions),
        "result unresolved_regions";
        max_length=plan.limits.max_multi_indices,
        cancel_check=cancel_check)
    unresolved = ROSparseUnresolvedRegion[]
    sizehint!(unresolved, length(raw_unresolved))
    for (position, item) in enumerate(raw_unresolved)
        _ross_v2_cancel_checkpoint(cancel_check, position)
        push!(unresolved, _ross_v2_parse_unresolved(item;
            dimension=dimension, max_points=plan.limits.max_points,
            cancel_check=cancel_check))
    end
    counters = _ross_v2_get(payload, :counters)
    _ross_v2_require_keys(counters, (
        :evaluated_point_count, :valid_point_count, :invalid_point_count,
        :interpolation_work_consumed, :payload_scalar_count,
        :backend_work_unit_count), "v2 result counters")
    evaluated_count = _ross_v2_int(
        _ross_v2_get(counters, :evaluated_point_count),
        "evaluated_point_count")
    valid_count = _ross_v2_int(
        _ross_v2_get(counters, :valid_point_count), "valid_point_count")
    invalid_count = _ross_v2_int(
        _ross_v2_get(counters, :invalid_point_count), "invalid_point_count")
    interpolation_work = _ross_v2_int(
        _ross_v2_get(counters, :interpolation_work_consumed),
        "interpolation_work_consumed")
    payload_count = _ross_v2_int(
        _ross_v2_get(counters, :payload_scalar_count),
        "payload_scalar_count")
    backend_count = _ross_v2_int(
        _ross_v2_get(counters, :backend_work_unit_count),
        "backend_work_unit_count")
    0 <= evaluated_count <= plan.limits.max_points || throw(ArgumentError(
        "result evaluated_point_count exceeds the declared budget"))
    0 <= valid_count <= evaluated_count || throw(ArgumentError(
        "result valid_point_count is out of range"))
    0 <= invalid_count <= evaluated_count || throw(ArgumentError(
        "result invalid_point_count is out of range"))
    BigInt(valid_count) + BigInt(invalid_count) == evaluated_count ||
        throw(ArgumentError("result validity counts do not add up"))
    0 <= interpolation_work <= plan.limits.max_work ||
        throw(ArgumentError(
            "result interpolation work exceeds the declared budget"))
    0 <= payload_count <= plan.limits.max_payload_scalars ||
        throw(ArgumentError(
            "result payload scalar count exceeds the declared budget"))
    0 <= backend_count <= plan.limits.max_multi_indices ||
        throw(ArgumentError(
            "result backend work count exceeds the declared budget"))
    max_indicator_raw = _ross_v2_get(payload, :max_active_indicator)
    restored = ROSparseSamplingResultV2(
        _ROSS_V2_TOKEN, RO_SPARSE_RO_RESULT_VERSION,
        _ross_v2_hash(_ross_v2_get(payload, :plan_sha256),
            "result plan_sha256"),
        _ross_v2_hash(_ross_v2_get(payload, :sampling_spec_sha256),
            "result sampling_spec_sha256"),
        _ross_v2_hash(_ross_v2_get(payload, :terminal_state_sha256),
            "terminal_state_sha256"),
        Symbol(_ross_v2_string(
            _ross_v2_get(payload, :status), "result status")),
        Symbol(_ross_v2_string(
            _ross_v2_get(payload, :stopping_reason), "stopping_reason")),
        Symbol(_ross_v2_string(
            _ross_v2_get(payload, :evidence_scope), "evidence_scope")),
        accepted,
        active,
        refinements,
        records,
        samples,
        unresolved,
        evaluated_count,
        valid_count,
        invalid_count,
        interpolation_work,
        payload_count,
        backend_count,
        max_indicator_raw === nothing ? nothing :
            _ross_v2_float(max_indicator_raw, "max_active_indicator"),
        _ross_v2_hash(_ross_v2_get(payload, :result_sha256),
            "result_sha256"); cancel_check=cancel_check)
    validate_ro_sparse_result_v2(
        plan, terminal_state, restored; cancel_check=cancel_check,
        validate_state=validate_terminal_state)
    _ross_v2_validate_source_encoding(
        raw,
        merge(_ross_v2_result_body(restored),
            (result_sha256=restored.result_sha256,)),
        "v2 sparse result", cancel_check)
    cancel_check()
    return restored
end
