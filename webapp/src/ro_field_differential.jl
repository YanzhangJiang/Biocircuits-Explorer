const RO_FIELD_DIFFERENTIAL_ANALYSIS_VERSION =
    "bne-ro-field-differential-analysis/v1.0.0"
const RO_FIELD_DIFFERENTIAL_REQUEST_VERSION =
    "bne-ro-field-differential-request/v1.0.0"
const RO_FIELD_DIFFERENTIAL_ALGORITHM_VERSION =
    "bne-ro-field-finite-difference/v1.0.0"

const _ROFDA_TOP_LEVEL_KEYS = Set((
    "schema_version", "analysis_id", "analysis_sha256",
    "source_field_sha256", "source_data_sha256", "coordinate_contract",
    "integrability", "curvature", "synergy", "evidence",
))
const _ROFDA_COORDINATE_KEYS = Set((
    "axis_order", "output_order", "log_basis", "derivative_semantics",
))
const _ROFDA_INTEGRABILITY_KEYS = Set((
    "status", "complete", "input_dimension", "output_count",
    "total_face_count", "evaluated_face_count", "invalid_face_count",
    "violating_face_count", "absolute_tolerance", "relative_tolerance",
    "pair_summaries",
))
const _ROFDA_PAIR_KEYS = Set((
    "axis_pair", "total_face_count", "evaluated_face_count",
    "invalid_face_count", "violating_face_count", "max_abs_circulation",
    "max_abs_mixed_partial_mismatch", "max_abs_output_edge_residual",
    "max_normalized_residual", "worst_face_base_index", "worst_output_index",
))
const _ROFDA_CURVATURE_KEYS = Set((
    "status", "complete", "cell_shape", "total_cell_count",
    "evaluated_cell_count", "invalid_cell_count", "flatten_order",
    "validity", "gradient_jacobian_shape", "gradient_jacobian_values",
    "symmetric_hessian_shape", "symmetric_hessian_values",
    "mixed_output_curvature_shape", "mixed_output_curvature_values",
    "antisymmetry_residual_shape", "antisymmetry_residual_values",
    "hessian_eigenvalues_shape", "hessian_eigenvalues_values",
))
const _ROFDA_SYNERGY_KEYS = Set((
    "policy", "threshold", "classification_shape", "classification_values",
    "pair_summaries", "interpretation",
))
const _ROFDA_SYNERGY_PAIR_KEYS = Set((
    "axis_pair", "output_index", "evaluated_cell_count", "positive_count",
    "negative_count", "neutral_count", "unknown_gap_count",
))
const _ROFDA_EVIDENCE_KEYS = Set((
    "evidence_class", "claim_scope", "integrability_claim",
    "validity_policy", "limitations",
))
const _ROFDA_INTEGRABILITY_STATUSES = Set((
    "consistent_on_tested_grid", "discrete_inconsistent", "unknown_gap",
    "insufficient_grid",
))
const _ROFDA_CURVATURE_STATUSES = Set((
    "complete", "partial", "no_valid_cells", "insufficient_grid",
))
const _ROFDA_CLASSIFICATIONS = Set((
    "synergistic_under_policy", "antagonistic_under_policy",
    "neutral_under_policy", "unknown_gap", "not_applicable",
))

function _rofda_exact_keys(raw, expected::Set{String}, path::AbstractString)
    raw isa AbstractDict || throw(ArgumentError("$path must be an object"))
    observed = Set(String(key) for key in keys(raw))
    observed == expected || throw(ArgumentError(
        "$path fields must be exactly $(sort!(collect(expected)))"))
    return raw
end

function _rofda_int(raw, path::AbstractString; minimum::Int=0)
    raw isa Integer && !(raw isa Bool) || throw(ArgumentError(
        "$path must be an integer"))
    value = try
        Int(raw)
    catch
        throw(ArgumentError("$path is outside the supported integer range"))
    end
    value >= minimum || throw(ArgumentError("$path must be at least $minimum"))
    return value
end

function _rofda_number(raw, path::AbstractString; nullable::Bool=false)
    nullable && raw === nothing && return nothing
    raw isa Real && !(raw isa Bool) || throw(ArgumentError(
        "$path must be a finite number$(nullable ? " or null" : "")"))
    value = Float64(raw)
    isfinite(value) || throw(ArgumentError("$path must be finite"))
    return value
end

function _rofda_string(raw, path::AbstractString)
    raw isa AbstractString || throw(ArgumentError("$path must be a string"))
    value = String(raw)
    isempty(value) && throw(ArgumentError("$path must not be empty"))
    return value
end

function _rofda_sha(raw, path::AbstractString)
    value = _rofda_string(raw, path)
    occursin(r"^[0-9a-f]{64}$", value) || throw(ArgumentError(
        "$path must be a lowercase SHA-256 value"))
    return value
end

function _rofda_shape(raw, path::AbstractString; rank::Union{Nothing,Int}=nothing)
    raw isa AbstractVector || throw(ArgumentError("$path must be an array"))
    values = Int[_rofda_int(value, "$path[$index]")
        for (index, value) in enumerate(raw)]
    rank === nothing || length(values) == rank || throw(ArgumentError(
        "$path must have rank $rank"))
    return values
end

function _rofda_product(shape::AbstractVector{<:Integer}, path::AbstractString)
    value = prod(BigInt(extent) for extent in shape)
    value <= typemax(Int) || throw(ArgumentError("$path product is too large"))
    return Int(value)
end

_rofda_flatten(array) =
    vec(Any[array[index] for index in
        _ro_field_row_major_indices(size(array))])

_rofda_flatten_finite_or_null(array) =
    vec(Any[isfinite(array[index]) ? Float64(array[index]) : nothing
        for index in _ro_field_row_major_indices(size(array))])

function _rofda_sampled_field(document)
    data = document["data"]
    data["sampling_scheme"] == "cartesian_product" || throw(ArgumentError(
        "differential analysis requires cartesian_product sampled data"))
    storage = document["coverage"]["storage"]
    storage["mode"] == "inline" && storage["complete"] === true ||
        throw(ArgumentError(
            "differential analysis requires a complete inline sampled artifact"))

    grid_shape = Int.(data["grid_shape"])
    input_count = length(grid_shape)
    output_count = length(document["outputs"]["output_order"])
    coordinates = Vector{Vector{Float64}}(
        [Float64.(axis) for axis in data["axis_coordinates"]])
    validity_flat = Bool.(data["validity"])
    point_count = _rofda_product(grid_shape, "data.grid_shape")
    length(validity_flat) == point_count || error(
        "validated sampled artifact has inconsistent validity length")

    output = fill(NaN, (Tuple(grid_shape)..., output_count))
    reaction_orders = fill(NaN,
        (Tuple(grid_shape)..., output_count, input_count))
    validity = falses(Tuple(grid_shape))
    regime_ids = zeros(Int, Tuple(grid_shape))
    output_values = data["output_values"]
    reaction_values = data["reaction_order_values"]
    for (position, grid_index) in enumerate(
        _ro_field_row_major_indices(Tuple(grid_shape)))
        point = Tuple(grid_index)
        valid = validity_flat[position]
        validity[grid_index] = valid
        regime_ids[grid_index] = valid ? 1 : 0
        output_start = (position - 1) * output_count
        reaction_start = (position - 1) * output_count * input_count
        valid || continue
        for output_index in 1:output_count
            output[point..., output_index] =
                Float64(output_values[output_start + output_index])
            for axis in 1:input_count
                reaction_orders[point..., output_index, axis] = Float64(
                    reaction_values[reaction_start +
                        (output_index - 1) * input_count + axis])
            end
        end
    end
    return SampledReactionOrderField(
        collect(1:input_count), coordinates, collect(1:output_count),
        zeros(input_count), output, reaction_orders, validity, regime_ids)
end

function _rofda_integrability_payload(certificate)
    pairs = Dict{String,Any}[]
    for summary in certificate.pair_summaries
        push!(pairs, Dict{String,Any}(
            "axis_pair" => collect(summary.axis_pair),
            "total_face_count" => summary.total_face_count,
            "evaluated_face_count" => summary.evaluated_face_count,
            "invalid_face_count" => summary.invalid_face_count,
            "violating_face_count" => summary.violating_face_count,
            "max_abs_circulation" => summary.max_abs_circulation,
            "max_abs_mixed_partial_mismatch" =>
                summary.max_abs_mixed_partial_mismatch,
            "max_abs_output_edge_residual" =>
                summary.max_abs_output_edge_residual,
            "max_normalized_residual" => summary.max_normalized_residual,
            "worst_face_base_index" => summary.worst_face_base_index,
            "worst_output_index" => summary.worst_output_index,
        ))
    end
    return Dict{String,Any}(
        "status" => String(certificate.status),
        "complete" => certificate.complete,
        "input_dimension" => certificate.input_dimension,
        "output_count" => certificate.output_count,
        "total_face_count" => certificate.total_face_count,
        "evaluated_face_count" => certificate.evaluated_face_count,
        "invalid_face_count" => certificate.invalid_face_count,
        "violating_face_count" => certificate.violating_face_count,
        "absolute_tolerance" => certificate.absolute_tolerance,
        "relative_tolerance" => certificate.relative_tolerance,
        "pair_summaries" => pairs,
    )
end

function _rofda_curvature_payload(curvature)
    return Dict{String,Any}(
        "status" => String(curvature.status),
        "complete" => curvature.complete,
        "cell_shape" => curvature.cell_shape,
        "total_cell_count" => curvature.total_cell_count,
        "evaluated_cell_count" => curvature.evaluated_cell_count,
        "invalid_cell_count" => curvature.invalid_cell_count,
        "flatten_order" => "row_major_last_axis_fastest",
        "validity" => Bool.(_rofda_flatten(curvature.validity)),
        "gradient_jacobian_shape" => collect(size(curvature.gradient_jacobian)),
        "gradient_jacobian_values" =>
            _rofda_flatten_finite_or_null(curvature.gradient_jacobian),
        "symmetric_hessian_shape" => collect(size(curvature.symmetric_hessian)),
        "symmetric_hessian_values" =>
            _rofda_flatten_finite_or_null(curvature.symmetric_hessian),
        "mixed_output_curvature_shape" =>
            collect(size(curvature.mixed_output_curvature)),
        "mixed_output_curvature_values" =>
            _rofda_flatten_finite_or_null(curvature.mixed_output_curvature),
        "antisymmetry_residual_shape" =>
            collect(size(curvature.antisymmetry_residual)),
        "antisymmetry_residual_values" =>
            _rofda_flatten_finite_or_null(curvature.antisymmetry_residual),
        "hessian_eigenvalues_shape" =>
            collect(size(curvature.hessian_eigenvalues)),
        "hessian_eigenvalues_values" =>
            _rofda_flatten_finite_or_null(curvature.hessian_eigenvalues),
    )
end

function _rofda_synergy_payload(synergy)
    summaries = Dict{String,Any}[]
    for summary in synergy.pair_summaries
        push!(summaries, Dict{String,Any}(
            "axis_pair" => collect(summary.axis_pair),
            "output_index" => summary.output_index,
            "evaluated_cell_count" => summary.evaluated_cell_count,
            "positive_count" => summary.positive_count,
            "negative_count" => summary.negative_count,
            "neutral_count" => summary.neutral_count,
            "unknown_gap_count" => summary.unknown_gap_count,
        ))
    end
    return Dict{String,Any}(
        "policy" => String(synergy.policy),
        "threshold" => synergy.threshold,
        "classification_shape" => collect(size(synergy.classification)),
        "classification_values" =>
            String.(_rofda_flatten(synergy.classification)),
        "pair_summaries" => summaries,
        "interpretation" =>
            "coordinate_scale_and_window_dependent_not_causal_or_mechanistic",
    )
end

"""
Create a deterministic, separately identified finite-grid diagnostic artifact
from a complete inline sampled RO field. The source artifact is never mutated
or reinterpreted. A consistent result is evidence only on the tested grid.
"""
function analyze_ro_field_differential(
    raw_document;
    absolute_tolerance::Real=1e-8,
    relative_tolerance::Real=1e-6,
    synergy_threshold::Real=1e-8,
    max_faces::Integer=100_000,
    max_face_output_evaluations::Integer=1_000_000,
    max_cells::Integer=100_000,
    max_corner_visits::Integer=1_000_000,
)
    document = Dict{String,Any}(_materialize(raw_document))
    validate_ro_field_document!(document)
    document["representation"] == "sampled_grid" || throw(ArgumentError(
        "finite-difference analysis requires a sampled_grid RO field"))
    field = _rofda_sampled_field(document)
    certificate = certify_sampled_ro_integrability(
        field;
        absolute_tolerance=absolute_tolerance,
        relative_tolerance=relative_tolerance,
        max_faces=max_faces,
        max_face_output_evaluations=max_face_output_evaluations,
    )
    curvature = estimate_sampled_ro_curvature(
        field; max_cells=max_cells, max_corner_visits=max_corner_visits)
    synergy = classify_finite_window_synergy(
        curvature; threshold=synergy_threshold)

    analysis = Dict{String,Any}(
        "schema_version" => RO_FIELD_DIFFERENTIAL_ANALYSIS_VERSION,
        "analysis_id" => "pending",
        "analysis_sha256" => "0"^64,
        "source_field_sha256" => ro_field_artifact_sha256(document),
        "source_data_sha256" => ro_field_data_sha256(document),
        "coordinate_contract" => Dict{String,Any}(
            "axis_order" => document["domain"]["axis_order"],
            "output_order" => document["outputs"]["output_order"],
            "log_basis" => document["domain"]["log_basis"],
            "derivative_semantics" =>
                "d_output_log_ratio_per_input_log_ratio_in_declared_coordinates",
        ),
        "integrability" => _rofda_integrability_payload(certificate),
        "curvature" => _rofda_curvature_payload(curvature),
        "synergy" => _rofda_synergy_payload(synergy),
        "evidence" => Dict{String,Any}(
            "evidence_class" => "sampled_finite_difference_diagnostic",
            "claim_scope" => "declared_finite_grid_and_tolerances_only",
            "integrability_claim" => String(certificate.status),
            "validity_policy" => "invalid_is_gap",
            "limitations" => String[
                "A consistent finite grid is not a continuum integrability proof.",
                "Finite differences depend on the declared coordinates, scale, and cell windows.",
                "The synergy label is an explicit cross-curvature convention, not a causal or mechanistic claim.",
            ],
        ),
    )
    identity = deepcopy(analysis)
    delete!(identity, "analysis_id")
    delete!(identity, "analysis_sha256")
    analysis_hash = _canonical_hash(identity)
    analysis["analysis_id"] = "ro-differential-$(analysis_hash[1:24])"
    analysis["analysis_sha256"] = analysis_hash
    validate_ro_field_differential_analysis!(analysis)
    return analysis
end

function _rofda_validate_nullable_vector(raw, expected_length::Int,
                                         path::AbstractString)
    raw isa AbstractVector || throw(ArgumentError("$path must be an array"))
    length(raw) == expected_length || throw(ArgumentError(
        "$path length does not match its shape"))
    for (index, value) in enumerate(raw)
        _rofda_number(value, "$path[$index]"; nullable=true)
    end
    return nothing
end

function _rofda_validate_integrability!(raw, input_dimension::Int,
                                        output_count::Int)
    value = _rofda_exact_keys(raw, _ROFDA_INTEGRABILITY_KEYS,
        "analysis.integrability")
    status = _rofda_string(value["status"], "analysis.integrability.status")
    status in _ROFDA_INTEGRABILITY_STATUSES || throw(ArgumentError(
        "analysis.integrability.status is unsupported"))
    value["complete"] isa Bool || throw(ArgumentError(
        "analysis.integrability.complete must be Boolean"))
    _rofda_int(value["input_dimension"],
        "analysis.integrability.input_dimension"; minimum=1) == input_dimension ||
        throw(ArgumentError("integrability input dimension is inconsistent"))
    _rofda_int(value["output_count"],
        "analysis.integrability.output_count"; minimum=1) == output_count ||
        throw(ArgumentError("integrability output count is inconsistent"))
    total = _rofda_int(value["total_face_count"],
        "analysis.integrability.total_face_count")
    evaluated = _rofda_int(value["evaluated_face_count"],
        "analysis.integrability.evaluated_face_count")
    invalid = _rofda_int(value["invalid_face_count"],
        "analysis.integrability.invalid_face_count")
    violating = _rofda_int(value["violating_face_count"],
        "analysis.integrability.violating_face_count")
    evaluated + invalid == total || throw(ArgumentError(
        "integrability evaluated and invalid counts do not cover all faces"))
    violating <= evaluated || throw(ArgumentError(
        "integrability violating faces exceed evaluated faces"))
    _rofda_number(value["absolute_tolerance"],
        "analysis.integrability.absolute_tolerance") > 0 ||
        throw(ArgumentError("integrability absolute tolerance must be positive"))
    _rofda_number(value["relative_tolerance"],
        "analysis.integrability.relative_tolerance") >= 0 ||
        throw(ArgumentError("integrability relative tolerance must be nonnegative"))

    pairs = value["pair_summaries"]
    pairs isa AbstractVector || throw(ArgumentError(
        "analysis.integrability.pair_summaries must be an array"))
    length(pairs) == input_dimension * (input_dimension - 1) ÷ 2 ||
        throw(ArgumentError("integrability pair count is inconsistent"))
    pair_total = 0
    pair_evaluated = 0
    pair_invalid = 0
    pair_violating = 0
    seen_pairs = Set{Tuple{Int,Int}}()
    for (index, raw_pair) in enumerate(pairs)
        path = "analysis.integrability.pair_summaries[$index]"
        pair = _rofda_exact_keys(raw_pair, _ROFDA_PAIR_KEYS, path)
        axes = _rofda_shape(pair["axis_pair"], "$path.axis_pair"; rank=2)
        1 <= axes[1] < axes[2] <= input_dimension || throw(ArgumentError(
            "$path.axis_pair is invalid"))
        axis_pair = (axes[1], axes[2])
        axis_pair in seen_pairs && throw(ArgumentError(
            "$path.axis_pair is duplicated"))
        push!(seen_pairs, axis_pair)
        counts = Int[_rofda_int(pair[key], "$path.$key") for key in (
            "total_face_count", "evaluated_face_count", "invalid_face_count",
            "violating_face_count")]
        counts[2] + counts[3] == counts[1] || throw(ArgumentError(
            "$path counts are inconsistent"))
        counts[4] <= counts[2] || throw(ArgumentError(
            "$path violating count exceeds evaluated count"))
        pair_total += counts[1]
        pair_evaluated += counts[2]
        pair_invalid += counts[3]
        pair_violating += counts[4]
        for key in ("max_abs_circulation", "max_abs_mixed_partial_mismatch",
                    "max_abs_output_edge_residual", "max_normalized_residual")
            residual = _rofda_number(pair[key], "$path.$key"; nullable=true)
            residual === nothing || residual >= 0 || throw(ArgumentError(
                "$path.$key must be nonnegative or null"))
        end
        base = pair["worst_face_base_index"]
        base isa AbstractVector || throw(ArgumentError(
            "$path.worst_face_base_index must be an array"))
        isempty(base) || length(base) == input_dimension || throw(ArgumentError(
            "$path.worst_face_base_index rank is inconsistent"))
        for (base_index, coordinate) in enumerate(base)
            _rofda_int(coordinate,
                "$path.worst_face_base_index[$base_index]"; minimum=1)
        end
        worst_output = pair["worst_output_index"]
        worst_output === nothing ||
            1 <= _rofda_int(worst_output, "$path.worst_output_index";
                minimum=1) <= output_count || throw(ArgumentError(
                    "$path.worst_output_index is invalid"))
    end
    (pair_total, pair_evaluated, pair_invalid, pair_violating) ==
        (total, evaluated, invalid, violating) || throw(ArgumentError(
            "integrability pair counts disagree with aggregate counts"))
    expected_complete = total > 0 && invalid == 0
    value["complete"] == expected_complete || throw(ArgumentError(
        "integrability complete flag is inconsistent"))
    expected_status = total == 0 ? "insufficient_grid" :
        violating > 0 ? "discrete_inconsistent" :
        invalid > 0 ? "unknown_gap" : "consistent_on_tested_grid"
    status == expected_status || throw(ArgumentError(
        "integrability status is inconsistent with counts"))
    return status
end

function _rofda_validate_curvature!(raw, cell_rank::Int, output_count::Int)
    value = _rofda_exact_keys(raw, _ROFDA_CURVATURE_KEYS,
        "analysis.curvature")
    status = _rofda_string(value["status"], "analysis.curvature.status")
    status in _ROFDA_CURVATURE_STATUSES || throw(ArgumentError(
        "analysis.curvature.status is unsupported"))
    value["complete"] isa Bool || throw(ArgumentError(
        "analysis.curvature.complete must be Boolean"))
    cell_shape = _rofda_shape(value["cell_shape"],
        "analysis.curvature.cell_shape"; rank=cell_rank)
    cell_count = _rofda_product(cell_shape, "analysis.curvature.cell_shape")
    total = _rofda_int(value["total_cell_count"],
        "analysis.curvature.total_cell_count")
    total == cell_count || throw(ArgumentError(
        "curvature total_cell_count does not match cell_shape"))
    evaluated = _rofda_int(value["evaluated_cell_count"],
        "analysis.curvature.evaluated_cell_count")
    invalid = _rofda_int(value["invalid_cell_count"],
        "analysis.curvature.invalid_cell_count")
    evaluated + invalid == total || throw(ArgumentError(
        "curvature evaluated and invalid counts do not cover all cells"))
    value["complete"] == (total > 0 && invalid == 0) || throw(ArgumentError(
        "curvature complete flag is inconsistent"))
    expected_status = total == 0 ? "insufficient_grid" :
        evaluated == 0 ? "no_valid_cells" : invalid == 0 ? "complete" : "partial"
    status == expected_status || throw(ArgumentError(
        "curvature status is inconsistent with counts"))
    value["flatten_order"] == "row_major_last_axis_fastest" ||
        throw(ArgumentError("curvature flatten_order is unsupported"))
    validity = value["validity"]
    validity isa AbstractVector && length(validity) == total &&
        all(item -> item isa Bool, validity) || throw(ArgumentError(
            "curvature validity is inconsistent"))
    count(identity, validity) == evaluated || throw(ArgumentError(
        "curvature validity count is inconsistent"))

    tensor_specs = (
        ("gradient_jacobian",
            [cell_shape; output_count; cell_rank; cell_rank]),
        ("symmetric_hessian",
            [cell_shape; output_count; cell_rank; cell_rank]),
        ("mixed_output_curvature",
            [cell_shape; output_count; cell_rank; cell_rank]),
        ("antisymmetry_residual", [cell_shape; output_count]),
        ("hessian_eigenvalues", [cell_shape; output_count; cell_rank]),
    )
    for (name, expected_shape) in tensor_specs
        shape_path = "analysis.curvature.$(name)_shape"
        shape = _rofda_shape(value["$(name)_shape"], shape_path)
        shape == expected_shape || throw(ArgumentError(
            "$shape_path is inconsistent"))
        _rofda_validate_nullable_vector(value["$(name)_values"],
            _rofda_product(shape, shape_path),
            "analysis.curvature.$(name)_values")
    end

    tensor_values = Dict(
        "gradient_jacobian" => value["gradient_jacobian_values"],
        "symmetric_hessian" => value["symmetric_hessian_values"],
        "mixed_output_curvature" => value["mixed_output_curvature_values"],
        "antisymmetry_residual" => value["antisymmetry_residual_values"],
        "hessian_eigenvalues" => value["hessian_eigenvalues_values"],
    )
    block_sizes = Dict(
        "gradient_jacobian" => output_count * cell_rank * cell_rank,
        "symmetric_hessian" => output_count * cell_rank * cell_rank,
        "mixed_output_curvature" => output_count * cell_rank * cell_rank,
        "antisymmetry_residual" => output_count,
        "hessian_eigenvalues" => output_count * cell_rank,
    )
    for cell_position in 1:cell_count
        valid = validity[cell_position]
        for name in keys(tensor_values)
            block_size = block_sizes[name]
            first_index = (cell_position - 1) * block_size + 1
            last_index = cell_position * block_size
            block = @view tensor_values[name][first_index:last_index]
            if !valid
                all(isnothing, block) || throw(ArgumentError(
                    "invalid curvature cells must retain null tensor blocks"))
            elseif name != "mixed_output_curvature"
                all(item -> item !== nothing, block) || throw(ArgumentError(
                    "valid curvature cells require finite tensor blocks"))
            else
                for output in 1:output_count, left_axis in 1:cell_rank,
                    right_axis in 1:cell_rank
                    block_index = (output - 1) * cell_rank * cell_rank +
                        (left_axis - 1) * cell_rank + right_axis
                    item = block[block_index]
                    if left_axis == right_axis
                        item === nothing || throw(ArgumentError(
                            "mixed-curvature diagonal entries must be null"))
                    else
                        item !== nothing || throw(ArgumentError(
                            "valid mixed-curvature off-diagonal entries must be finite"))
                    end
                end
            end
        end
    end
    return cell_shape, cell_count, evaluated, Bool.(validity)
end

function _rofda_validate_synergy!(raw, cell_shape, cell_count::Int,
                                  input_dimension::Int, output_count::Int,
                                  curvature_validity::AbstractVector{Bool})
    value = _rofda_exact_keys(raw, _ROFDA_SYNERGY_KEYS,
        "analysis.synergy")
    value["policy"] == "positive_log_cross_curvature" || throw(ArgumentError(
        "analysis.synergy.policy is unsupported"))
    _rofda_number(value["threshold"], "analysis.synergy.threshold") >= 0 ||
        throw(ArgumentError("analysis.synergy.threshold must be nonnegative"))
    shape = _rofda_shape(value["classification_shape"],
        "analysis.synergy.classification_shape")
    shape == [cell_shape; output_count; input_dimension; input_dimension] ||
        throw(ArgumentError("synergy classification shape is inconsistent"))
    classifications = value["classification_values"]
    classifications isa AbstractVector &&
        length(classifications) == _rofda_product(shape,
            "analysis.synergy.classification_shape") || throw(ArgumentError(
                "synergy classification length is inconsistent"))
    all(item -> item isa AbstractString &&
        String(item) in _ROFDA_CLASSIFICATIONS, classifications) ||
        throw(ArgumentError(
            "synergy classification contains an unsupported label"))
    classification_block_size = output_count * input_dimension * input_dimension
    for cell_position in 1:cell_count, output in 1:output_count,
        left_axis in 1:input_dimension, right_axis in 1:input_dimension
        block_index = (output - 1) * input_dimension * input_dimension +
            (left_axis - 1) * input_dimension + right_axis
        index = (cell_position - 1) * classification_block_size + block_index
        label = String(classifications[index])
        if left_axis == right_axis
            label == "not_applicable" || throw(ArgumentError(
                "synergy diagonal classifications must be not_applicable"))
        elseif !curvature_validity[cell_position]
            label == "unknown_gap" || throw(ArgumentError(
                "invalid curvature cells must retain unknown synergy gaps"))
        else
            label in ("synergistic_under_policy",
                "antagonistic_under_policy", "neutral_under_policy") ||
                throw(ArgumentError(
                    "valid curvature cells require a finite-window policy label"))
        end
    end
    value["interpretation"] ==
        "coordinate_scale_and_window_dependent_not_causal_or_mechanistic" ||
        throw(ArgumentError("synergy interpretation is unsupported"))
    pairs = value["pair_summaries"]
    pairs isa AbstractVector && length(pairs) == output_count *
        input_dimension * (input_dimension - 1) ÷ 2 || throw(ArgumentError(
            "synergy pair summary count is inconsistent"))
    seen = Set{Tuple{Int,Int,Int}}()
    for (index, raw_pair) in enumerate(pairs)
        path = "analysis.synergy.pair_summaries[$index]"
        pair = _rofda_exact_keys(raw_pair, _ROFDA_SYNERGY_PAIR_KEYS, path)
        axes = _rofda_shape(pair["axis_pair"], "$path.axis_pair"; rank=2)
        1 <= axes[1] < axes[2] <= input_dimension || throw(ArgumentError(
            "$path.axis_pair is invalid"))
        output = _rofda_int(pair["output_index"], "$path.output_index";
            minimum=1)
        output <= output_count || throw(ArgumentError(
            "$path.output_index is invalid"))
        key = (output, axes[1], axes[2])
        key in seen && throw(ArgumentError("$path duplicates a pair summary"))
        push!(seen, key)
        evaluated = _rofda_int(pair["evaluated_cell_count"],
            "$path.evaluated_cell_count")
        positive = _rofda_int(pair["positive_count"], "$path.positive_count")
        negative = _rofda_int(pair["negative_count"], "$path.negative_count")
        neutral = _rofda_int(pair["neutral_count"], "$path.neutral_count")
        unknown = _rofda_int(pair["unknown_gap_count"],
            "$path.unknown_gap_count")
        positive + negative + neutral == evaluated || throw(ArgumentError(
            "$path evaluated classification counts are inconsistent"))
        evaluated == count(identity, curvature_validity) &&
            evaluated + unknown == cell_count ||
            throw(ArgumentError("$path does not cover the curvature cells"))
    end
    return nothing
end

"""Strictly validate and re-hash one differential-analysis artifact."""
function validate_ro_field_differential_analysis!(raw)
    analysis = _rofda_exact_keys(raw, _ROFDA_TOP_LEVEL_KEYS, "analysis")
    analysis["schema_version"] == RO_FIELD_DIFFERENTIAL_ANALYSIS_VERSION ||
        throw(ArgumentError("unsupported differential-analysis schema version"))
    analysis_hash = _rofda_sha(analysis["analysis_sha256"],
        "analysis.analysis_sha256")
    _rofda_sha(analysis["source_field_sha256"],
        "analysis.source_field_sha256")
    _rofda_sha(analysis["source_data_sha256"],
        "analysis.source_data_sha256")
    analysis["analysis_id"] ==
        "ro-differential-$(analysis_hash[1:24])" || throw(ArgumentError(
            "analysis_id does not match analysis_sha256"))

    coordinate = _rofda_exact_keys(analysis["coordinate_contract"],
        _ROFDA_COORDINATE_KEYS, "analysis.coordinate_contract")
    axes = coordinate["axis_order"]
    outputs = coordinate["output_order"]
    axes isa AbstractVector && !isempty(axes) &&
        all(item -> item isa AbstractString && !isempty(item), axes) &&
        length(unique(String.(axes))) == length(axes) || throw(ArgumentError(
            "coordinate axis_order must be nonempty and unique"))
    outputs isa AbstractVector && !isempty(outputs) &&
        all(item -> item isa AbstractString && !isempty(item), outputs) &&
        length(unique(String.(outputs))) == length(outputs) || throw(ArgumentError(
            "coordinate output_order must be nonempty and unique"))
    coordinate["log_basis"] in ("log10", "natural_log") ||
        throw(ArgumentError("coordinate log_basis is unsupported"))
    coordinate["derivative_semantics"] ==
        "d_output_log_ratio_per_input_log_ratio_in_declared_coordinates" ||
        throw(ArgumentError("coordinate derivative semantics are unsupported"))
    input_dimension = length(axes)
    output_count = length(outputs)

    integrability_status = _rofda_validate_integrability!(
        analysis["integrability"], input_dimension, output_count)
    cell_shape, cell_count, _, curvature_validity = _rofda_validate_curvature!(
        analysis["curvature"], input_dimension, output_count)
    _rofda_validate_synergy!(analysis["synergy"], cell_shape, cell_count,
        input_dimension, output_count, curvature_validity)

    evidence = _rofda_exact_keys(analysis["evidence"], _ROFDA_EVIDENCE_KEYS,
        "analysis.evidence")
    evidence["evidence_class"] == "sampled_finite_difference_diagnostic" ||
        throw(ArgumentError("analysis evidence_class is unsupported"))
    evidence["claim_scope"] == "declared_finite_grid_and_tolerances_only" ||
        throw(ArgumentError("analysis claim_scope is unsupported"))
    evidence["integrability_claim"] == integrability_status ||
        throw(ArgumentError(
            "analysis integrability claim disagrees with the certificate"))
    evidence["validity_policy"] == "invalid_is_gap" || throw(ArgumentError(
        "analysis validity_policy is unsupported"))
    limitations = evidence["limitations"]
    limitations isa AbstractVector && length(limitations) >= 3 &&
        all(item -> item isa AbstractString && !isempty(item), limitations) ||
        throw(ArgumentError("analysis evidence limitations are incomplete"))

    identity = deepcopy(analysis)
    delete!(identity, "analysis_id")
    delete!(identity, "analysis_sha256")
    _canonical_hash(identity) == analysis_hash || throw(ArgumentError(
        "differential-analysis content does not match analysis_sha256"))
    return raw
end

const _ROFDA_REQUEST_KEYS = Set(("schema_version", "ro_field", "options"))
const _ROFDA_OPTION_KEYS = Set((
    "absolute_tolerance", "relative_tolerance", "synergy_threshold",
    "max_faces", "max_face_output_evaluations", "max_cells",
    "max_corner_visits",
))

function _rofda_http_options(raw)
    raw === nothing && (raw = Dict{String,Any}())
    raw isa AbstractDict || throw(ArgumentError("options must be an object"))
    observed = Set(String(key) for key in keys(raw))
    isempty(setdiff(observed, _ROFDA_OPTION_KEYS)) || throw(ArgumentError(
        "options contains unsupported fields"))
    number(name, default; positive=false) = begin
        value = haskey(raw, name) ?
            _rofda_number(raw[name], "options.$name") : default
        in_range = positive ? value > 0 : value >= 0
        in_range || throw(ArgumentError(
            "options.$name is outside the supported range"))
        value
    end
    bounded_int(name, default, limit) = begin
        value = haskey(raw, name) ?
            _rofda_int(raw[name], "options.$name"; minimum=1) : default
        value <= limit || throw(ArgumentError(
            "options.$name exceeds the synchronous limit of $limit"))
        value
    end
    return Dict{String,Any}(
        "absolute_tolerance" => number("absolute_tolerance", 1e-8;
            positive=true),
        "relative_tolerance" => number("relative_tolerance", 1e-6),
        "synergy_threshold" => number("synergy_threshold", 1e-8),
        "max_faces" => bounded_int("max_faces", 100_000, 100_000),
        "max_face_output_evaluations" => bounded_int(
            "max_face_output_evaluations", 1_000_000, 1_000_000),
        "max_cells" => bounded_int("max_cells", 100_000, 100_000),
        "max_corner_visits" => bounded_int(
            "max_corner_visits", 1_000_000, 1_000_000),
    )
end

"""Canonical v1-only HTTP adapter for a separate finite-grid analysis."""
function handle_ro_field_differential(req)
    body = _materialize(read_json(req))
    body = _rofda_exact_keys(body, _ROFDA_REQUEST_KEYS, "request")
    body["schema_version"] == RO_FIELD_DIFFERENTIAL_REQUEST_VERSION ||
        throw(ArgumentError(
            "schema_version must be $(RO_FIELD_DIFFERENTIAL_REQUEST_VERSION)"))
    options = _rofda_http_options(body["options"])
    analysis = analyze_ro_field_differential(
        body["ro_field"];
        absolute_tolerance=options["absolute_tolerance"],
        relative_tolerance=options["relative_tolerance"],
        synergy_threshold=options["synergy_threshold"],
        max_faces=options["max_faces"],
        max_face_output_evaluations=options["max_face_output_evaluations"],
        max_cells=options["max_cells"],
        max_corner_visits=options["max_corner_visits"],
    )
    artifact = artifact_metadata(
        "ro_field_differential_analysis";
        input_hashes=Dict{String,Any}(
            "source_field_sha256" => analysis["source_field_sha256"],
            "source_data_sha256" => analysis["source_data_sha256"],
            "analysis_sha256" => analysis["analysis_sha256"],
        ),
        algorithm_name="finite_grid_ro_field_differential_analysis",
        config=options,
        warnings=String[
            "Finite-grid consistency is not a continuum integrability proof.",
            "Synergy labels are coordinate- and window-dependent conventions.",
        ],
    )
    return json_response(Dict{String,Any}(
        "analysis" => analysis,
        "artifact" => artifact,
    ))
end
