const RO_FIELD_EVIDENCE_IDENTITY_VERSION =
    "bne-ro-field-evidence-identity/v1.1.0"
const RO_SCIENTIFIC_SOURCE_IDENTITY_VERSION =
    "bne-ro-scientific-source-identity/v1.0.0"
const RO_LOCAL_IDENTIFIABILITY_VERSION =
    "bne-ro-local-identifiability/v1.1.0"
const RO_DELTA_METHOD_UNCERTAINTY_VERSION =
    "bne-ro-delta-method-uncertainty/v1.1.0"
const RO_SYNTHETIC_COVERAGE_VERSION =
    "bne-ro-synthetic-coverage/v1.0.0"
const RO_UNCERTAINTY_POPULATION_VERSION =
    "bne-ro-uncertainty-population/v1.1.0"
const RO_INTERVAL_COORDINATE_DEFINITION_VERSION =
    "bne-ro-interval-coordinate-definition/v1.0.0"
const RO_LOCAL_IDENTIFIABILITY_SCOPE =
    :local_declared_observation_model_only
const RO_COVARIANCE_MAX_SYMMETRY_RELATIVE_TOLERANCE = 1e-8

"""Unexported capability required by invariant-bearing value constructors."""
struct _ROUConstructionToken end
const _ROU_CONSTRUCTION_TOKEN = _ROUConstructionToken()

"""Hard dimension, allocation, factorization, and sorting limits."""
struct ROUncertaintyLimits
    max_inputs::Int
    max_parameters::Int
    max_outputs::Int
    max_observations::Int
    max_replicates::Int
    max_calibration_cases::Int
    max_quantiles::Int
    max_policy_depth::Int
    max_policy_elements::Int
    max_policy_bytes::Int
    max_metadata_bytes::Int
    max_matrix_elements::Int
    max_factorization_work::Int
    max_sort_work::Int

    function ROUncertaintyLimits(
        ::_ROUConstructionToken,
        values::Vararg{Int,14},
    )
        return new(values...)
    end
end

function ROUncertaintyLimits(;
    max_inputs::Integer=64,
    max_parameters::Integer=256,
    max_outputs::Integer=4_096,
    max_observations::Integer=4_096,
    max_replicates::Integer=100_000,
    max_calibration_cases::Integer=100_000,
    max_quantiles::Integer=1_024,
    max_policy_depth::Integer=32,
    max_policy_elements::Integer=10_000,
    max_policy_bytes::Integer=1_000_000,
    max_metadata_bytes::Integer=1_000_000,
    max_matrix_elements::Integer=2_000_000,
    max_factorization_work::Integer=2_000_000_000,
    max_sort_work::Integer=2_000_000_000,
)
    values = (
        max_inputs,
        max_parameters,
        max_outputs,
        max_observations,
        max_replicates,
        max_calibration_cases,
        max_quantiles,
        max_policy_depth,
        max_policy_elements,
        max_policy_bytes,
        max_metadata_bytes,
        max_matrix_elements,
        max_factorization_work,
        max_sort_work,
    )
    any(value -> value isa Bool, values) && throw(ArgumentError(
        "RO-field uncertainty limits must be integers, not Bool"))
    all(>(0), values) || throw(ArgumentError(
        "all RO-field uncertainty limits must be positive"))
    all(<=(typemax(Int)), values) || throw(ArgumentError(
        "all RO-field uncertainty limits must fit in Int"))
    return ROUncertaintyLimits(_ROU_CONSTRUCTION_TOKEN, Int.(values)...)
end

function _rou_rebuild_limits(limits::ROUncertaintyLimits)
    return ROUncertaintyLimits(
        max_inputs=limits.max_inputs,
        max_parameters=limits.max_parameters,
        max_outputs=limits.max_outputs,
        max_observations=limits.max_observations,
        max_replicates=limits.max_replicates,
        max_calibration_cases=limits.max_calibration_cases,
        max_quantiles=limits.max_quantiles,
        max_policy_depth=limits.max_policy_depth,
        max_policy_elements=limits.max_policy_elements,
        max_policy_bytes=limits.max_policy_bytes,
        max_metadata_bytes=limits.max_metadata_bytes,
        max_matrix_elements=limits.max_matrix_elements,
        max_factorization_work=limits.max_factorization_work,
        max_sort_work=limits.max_sort_work,
    )
end

"""Raised before an uncertainty analysis exceeds a declared hard limit."""
struct ROUncertaintyLimitExceeded <: Exception
    phase::Symbol
    requested::BigInt
    limit::Int
end

function Base.showerror(io::IO, err::ROUncertaintyLimitExceeded)
    print(io, "RO-field uncertainty analysis ", err.phase, " requires ",
        err.requested, ", exceeding limit=", err.limit)
end

"""A covariance is negative only inside its declared numerical grey zone."""
struct ROCovarianceNumericallyAmbiguous <: Exception
    label::String
    minimum_eigenvalue::Float64
    ambiguity_floor::Float64
end

function Base.showerror(io::IO, err::ROCovarianceNumericallyAmbiguous)
    print(io, err.label, " has minimum eigenvalue ",
        err.minimum_eigenvalue, " inside the +/-", err.ambiguity_floor,
        " numerical grey zone; no PSD projection was performed")
end

@inline function _rou_limit(phase::Symbol, requested::Integer, limit::Int)
    BigInt(requested) <= limit || throw(ROUncertaintyLimitExceeded(
        phase, BigInt(requested), limit))
    return nothing
end

@inline function _rou_cancel(cancel_check)
    cancel_check()
    return nothing
end

function _rou_cancel_after_entry(cancel_check)
    first_call = Ref(true)
    return function ()
        if first_call[]
            first_call[] = false
            return nothing
        end
        _rou_cancel(cancel_check)
        return nothing
    end
end

function _rou_string_tuple(
    values,
    label::AbstractString;
    unique_names=false,
    cancel_check=()->nothing,
)
    result = String[]
    for (position, value) in enumerate(values)
        value isa AbstractString || throw(ArgumentError(
            "$label entries must be strings"))
        normalized = String(value)
        isempty(strip(normalized)) && throw(ArgumentError(
            "$label entries must be non-empty"))
        push!(result, normalized)
        position % 1_024 == 0 && _rou_cancel(cancel_check)
    end
    unique_names && !allunique(result) && throw(ArgumentError(
        "$label entries must be unique"))
    return Tuple(result)
end

function _rou_preflight_sized_collection(
    values,
    label::AbstractString,
    phase::Symbol,
    maximum_count::Int,
)
    values isa Union{Tuple,AbstractVector} || throw(ArgumentError(
        "$label must be a sized Tuple or AbstractVector"))
    count = length(values)
    count >= 0 || throw(ArgumentError("$label has an invalid length"))
    _rou_limit(phase, BigInt(count), maximum_count)
    return count
end

@inline function _rou_charge_metadata_bytes!(
    byte_count::Base.RefValue{BigInt},
    additional::Integer,
    limits::ROUncertaintyLimits,
    phase::Symbol,
)
    additional >= 0 || throw(ArgumentError(
        "metadata byte charge must be nonnegative"))
    byte_count[] += BigInt(additional)
    _rou_limit(phase, byte_count[], limits.max_metadata_bytes)
    return nothing
end

function _rou_bounded_string(
    value,
    label::AbstractString,
    byte_count::Base.RefValue{BigInt},
    limits::ROUncertaintyLimits,
    phase::Symbol;
    cancel_check=()->nothing,
)
    value isa Union{String,SubString{String}} || throw(ArgumentError(
        "$label must be a String or SubString{String} with reliable byte length"))
    bytes = ncodeunits(value)
    _rou_charge_metadata_bytes!(byte_count, bytes, limits, phase)
    normalized = String(value)
    isempty(strip(normalized)) && throw(ArgumentError(
        "$label must be non-empty"))
    return normalized
end

function _rou_bounded_symbol_string(
    value::Symbol,
    label::AbstractString,
    byte_count::Base.RefValue{BigInt},
    limits::ROUncertaintyLimits,
    phase::Symbol;
    cancel_check=()->nothing,
)
    _rou_cancel(cancel_check)
    bytes = sizeof(value)
    _rou_charge_metadata_bytes!(byte_count, bytes, limits, phase)
    normalized = String(value)
    ncodeunits(normalized) == bytes || error(
        "$label Symbol byte length changed during String conversion")
    isempty(strip(normalized)) && throw(ArgumentError(
        "$label must be non-empty"))
    return normalized
end

function _rou_bounded_string_tuple(
    values,
    expected_count::Int,
    label::AbstractString,
    byte_count::Base.RefValue{BigInt},
    limits::ROUncertaintyLimits,
    phase::Symbol;
    unique_names=false,
    cancel_check=()->nothing,
)
    length(values) == expected_count || throw(DimensionMismatch(
        "$label must contain $expected_count entries"))
    result = Vector{String}(undef, expected_count)
    for (position, index) in enumerate(eachindex(values))
        position % 1_024 == 0 && _rou_cancel(cancel_check)
        result[position] = _rou_bounded_string(
            values[index], "$label[$position]", byte_count, limits, phase;
            cancel_check,
        )
    end
    unique_names && !allunique(result) && throw(ArgumentError(
        "$label entries must be unique"))
    return Tuple(result)
end

function _rou_bounded_finite_float_tuple(
    values,
    expected_count::Int,
    label::AbstractString;
    cancel_check=()->nothing,
)
    length(values) == expected_count || throw(DimensionMismatch(
        "$label must contain $expected_count entries"))
    result = Vector{Float64}(undef, expected_count)
    for (position, index) in enumerate(eachindex(values))
        position % 1_024 == 0 && _rou_cancel(cancel_check)
        value = values[index]
        value isa Float64 || throw(ArgumentError(
            "$label entries must use Float64 exactly"))
        normalized = _rou_canonical_float(value)
        isfinite(normalized) || throw(ArgumentError(
            "$label entries must be finite"))
        result[position] = normalized
    end
    return Tuple(result)
end

function _rou_sha256_string(value, label::AbstractString)
    value isa AbstractString || throw(ArgumentError("$label must be a string"))
    normalized = lowercase(String(value))
    occursin(r"^[0-9a-f]{64}$", normalized) || throw(ArgumentError(
        "$label must be a 64-character hexadecimal SHA-256"))
    return normalized
end

@inline function _rou_canonical_float(value)
    value isa Float64 || throw(ArgumentError(
        "scientific numeric evidence must use Float64 exactly"))
    return value == 0.0 ? 0.0 : value
end

function _rou_finite_float_tuple(values, label::AbstractString)
    result = Float64[]
    for value in values
        value isa Float64 || throw(ArgumentError(
            "$label entries must use Float64 exactly"))
        normalized = _rou_canonical_float(value)
        isfinite(normalized) || throw(ArgumentError(
            "$label entries must be finite"))
        push!(result, normalized)
    end
    return Tuple(result)
end

"""
Ordered coordinate, unit, and scale identity for a local RO-field evidence
artifact. Tuple order is semantic and therefore part of every derived hash.
"""
struct ROFieldEvidenceIdentity
    input_order::Tuple{Vararg{String}}
    input_units::Tuple{Vararg{String}}
    input_scales::Tuple{Vararg{String}}
    parameter_order::Tuple{Vararg{String}}
    parameter_units::Tuple{Vararg{String}}
    parameter_scales::Tuple{Vararg{String}}
    output_order::Tuple{Vararg{String}}
    output_units::Tuple{Vararg{String}}
    output_scales::Tuple{Vararg{String}}

    function ROFieldEvidenceIdentity(
        ::_ROUConstructionToken,
        values::Vararg{Tuple{Vararg{String}},9},
    )
        return new(values...)
    end
end

function ROFieldEvidenceIdentity(;
    input_order,
    input_units,
    input_scales,
    parameter_order,
    parameter_units,
    parameter_scales,
    output_order,
    output_units,
    output_scales,
    cancel_check=()->nothing,
    limits::ROUncertaintyLimits=ROUncertaintyLimits(),
)
    _rou_cancel(cancel_check)
    limits = _rou_rebuild_limits(limits)
    input_count = _rou_preflight_sized_collection(
        input_order, "input_order", :input_dimensions, limits.max_inputs)
    _rou_preflight_sized_collection(
        input_units, "input_units", :input_dimensions, limits.max_inputs) ==
        input_count || throw(DimensionMismatch(
            "input_units must match input_order"))
    _rou_preflight_sized_collection(
        input_scales, "input_scales", :input_dimensions, limits.max_inputs) ==
        input_count || throw(DimensionMismatch(
            "input_scales must match input_order"))
    parameter_count = _rou_preflight_sized_collection(
        parameter_order, "parameter_order", :parameter_dimensions,
        limits.max_parameters)
    _rou_preflight_sized_collection(
        parameter_units, "parameter_units", :parameter_dimensions,
        limits.max_parameters) == parameter_count || throw(DimensionMismatch(
            "parameter_units must match parameter_order"))
    _rou_preflight_sized_collection(
        parameter_scales, "parameter_scales", :parameter_dimensions,
        limits.max_parameters) == parameter_count || throw(DimensionMismatch(
            "parameter_scales must match parameter_order"))
    output_limit = min(limits.max_outputs, limits.max_observations)
    output_count = _rou_preflight_sized_collection(
        output_order, "output_order", :output_dimensions, output_limit)
    _rou_preflight_sized_collection(
        output_units, "output_units", :output_dimensions, output_limit) ==
        output_count || throw(DimensionMismatch(
            "output_units must match output_order"))
    _rou_preflight_sized_collection(
        output_scales, "output_scales", :output_dimensions, output_limit) ==
        output_count || throw(DimensionMismatch(
            "output_scales must match output_order"))
    byte_count = Ref(BigInt(0))
    inputs = _rou_bounded_string_tuple(
        input_order, input_count, "input_order", byte_count, limits,
        :identity_metadata_bytes; unique_names=true, cancel_check)
    input_unit_values = _rou_bounded_string_tuple(
        input_units, input_count, "input_units", byte_count, limits,
        :identity_metadata_bytes; cancel_check)
    input_scale_values = _rou_bounded_string_tuple(
        input_scales, input_count, "input_scales", byte_count, limits,
        :identity_metadata_bytes; cancel_check)
    parameters = _rou_bounded_string_tuple(
        parameter_order, parameter_count, "parameter_order", byte_count,
        limits, :identity_metadata_bytes; unique_names=true, cancel_check)
    parameter_unit_values = _rou_bounded_string_tuple(
        parameter_units, parameter_count, "parameter_units", byte_count,
        limits, :identity_metadata_bytes; cancel_check)
    parameter_scale_values = _rou_bounded_string_tuple(
        parameter_scales, parameter_count, "parameter_scales", byte_count,
        limits, :identity_metadata_bytes; cancel_check)
    outputs = _rou_bounded_string_tuple(
        output_order, output_count, "output_order", byte_count, limits,
        :identity_metadata_bytes; unique_names=true, cancel_check)
    output_unit_values = _rou_bounded_string_tuple(
        output_units, output_count, "output_units", byte_count, limits,
        :identity_metadata_bytes; cancel_check)
    output_scale_values = _rou_bounded_string_tuple(
        output_scales, output_count, "output_scales", byte_count, limits,
        :identity_metadata_bytes; cancel_check)
    isempty(parameters) && throw(ArgumentError(
        "parameter_order must contain at least one parameter"))
    isempty(outputs) && throw(ArgumentError(
        "output_order must contain at least one output/observation"))
    return ROFieldEvidenceIdentity(
        _ROU_CONSTRUCTION_TOKEN,
        inputs,
        input_unit_values,
        input_scale_values,
        parameters,
        parameter_unit_values,
        parameter_scale_values,
        outputs,
        output_unit_values,
        output_scale_values,
    )
end

function ro_field_evidence_identity_payload(identity::ROFieldEvidenceIdentity)
    return (
        schema_version=RO_FIELD_EVIDENCE_IDENTITY_VERSION,
        inputs=(
            order=identity.input_order,
            units=identity.input_units,
            scales=identity.input_scales,
        ),
        parameters=(
            order=identity.parameter_order,
            units=identity.parameter_units,
            scales=identity.parameter_scales,
        ),
        outputs=(
            order=identity.output_order,
            units=identity.output_units,
            scales=identity.output_scales,
        ),
    )
end

function _rou_rebuild_field_identity(
    identity::ROFieldEvidenceIdentity;
    cancel_check=()->nothing,
    limits::ROUncertaintyLimits=ROUncertaintyLimits(),
)
    rebuild_cancel_check = _rou_cancel_after_entry(cancel_check)
    return ROFieldEvidenceIdentity(
        input_order=identity.input_order,
        input_units=identity.input_units,
        input_scales=identity.input_scales,
        parameter_order=identity.parameter_order,
        parameter_units=identity.parameter_units,
        parameter_scales=identity.parameter_scales,
        output_order=identity.output_order,
        output_units=identity.output_units,
        output_scales=identity.output_scales,
        cancel_check=rebuild_cancel_check,
        limits=limits,
    )
end

"""
Mandatory provenance for a scientific uncertainty artifact. Hashes bind the
source field, its request, and network. The local point, nominal parameters,
observation schedule, solver, and algorithm revisions prevent reuse at a
different operating point or under a different numerical contract.
"""
struct ROScientificSourceIdentity
    source_field_sha256::String
    field_request_sha256::String
    network_sha256::String
    local_coordinates::Tuple{Vararg{Float64}}
    nominal_parameters::Tuple{Vararg{Float64}}
    observation_schedule::Tuple{Vararg{String}}
    solver_revision::String
    algorithm_revision::String

    function ROScientificSourceIdentity(
        ::_ROUConstructionToken,
        source_field_sha256::String,
        field_request_sha256::String,
        network_sha256::String,
        local_coordinates::Tuple{Vararg{Float64}},
        nominal_parameters::Tuple{Vararg{Float64}},
        observation_schedule::Tuple{Vararg{String}},
        solver_revision::String,
        algorithm_revision::String,
    )
        return new(
            source_field_sha256,
            field_request_sha256,
            network_sha256,
            local_coordinates,
            nominal_parameters,
            observation_schedule,
            solver_revision,
            algorithm_revision,
        )
    end
end

function ROScientificSourceIdentity(
    source_field_sha256::String,
    field_request_sha256::String,
    network_sha256::String,
    local_coordinates::Tuple{Vararg{Float64}},
    nominal_parameters::Tuple{Vararg{Float64}},
    observation_schedule::Tuple{Vararg{String}},
    solver_revision::String,
    algorithm_revision::String,
)
    return ROScientificSourceIdentity(
        source_field_sha256=source_field_sha256,
        field_request_sha256=field_request_sha256,
        network_sha256=network_sha256,
        local_coordinates=local_coordinates,
        nominal_parameters=nominal_parameters,
        observation_schedule=observation_schedule,
        solver_revision=solver_revision,
        algorithm_revision=algorithm_revision,
    )
end

function ROScientificSourceIdentity(;
    source_field_sha256,
    field_request_sha256,
    network_sha256,
    local_coordinates,
    nominal_parameters,
    observation_schedule,
    solver_revision,
    algorithm_revision,
    cancel_check=()->nothing,
    limits::ROUncertaintyLimits=ROUncertaintyLimits(),
)
    _rou_cancel(cancel_check)
    limits = _rou_rebuild_limits(limits)
    coordinate_count = _rou_preflight_sized_collection(
        local_coordinates, "local_coordinates", :input_dimensions,
        limits.max_inputs)
    parameter_count = _rou_preflight_sized_collection(
        nominal_parameters, "nominal_parameters", :parameter_dimensions,
        limits.max_parameters)
    schedule_limit = min(limits.max_outputs, limits.max_observations)
    schedule_count = _rou_preflight_sized_collection(
        observation_schedule, "observation_schedule", :output_dimensions,
        schedule_limit)
    byte_count = Ref(BigInt(0))
    _rou_charge_metadata_bytes!(
        byte_count,
        8 * BigInt(coordinate_count + parameter_count),
        limits,
        :source_metadata_bytes,
    )
    source_hash_text = _rou_bounded_string(
        source_field_sha256, "source_field_sha256", byte_count, limits,
        :source_metadata_bytes; cancel_check)
    request_hash_text = _rou_bounded_string(
        field_request_sha256, "field_request_sha256", byte_count, limits,
        :source_metadata_bytes; cancel_check)
    network_hash_text = _rou_bounded_string(
        network_sha256, "network_sha256", byte_count, limits,
        :source_metadata_bytes; cancel_check)
    solver = _rou_bounded_string(
        solver_revision, "solver_revision", byte_count, limits,
        :source_metadata_bytes; cancel_check)
    algorithm = _rou_bounded_string(
        algorithm_revision, "algorithm_revision", byte_count, limits,
        :source_metadata_bytes; cancel_check)
    schedule = _rou_bounded_string_tuple(
        observation_schedule, schedule_count, "observation_schedule",
        byte_count, limits, :source_metadata_bytes; cancel_check)
    coordinates = _rou_bounded_finite_float_tuple(
        local_coordinates, coordinate_count, "local_coordinates";
        cancel_check)
    parameters = _rou_bounded_finite_float_tuple(
        nominal_parameters, parameter_count, "nominal_parameters";
        cancel_check)
    return ROScientificSourceIdentity(
        _ROU_CONSTRUCTION_TOKEN,
        _rou_sha256_string(source_hash_text, "source_field_sha256"),
        _rou_sha256_string(request_hash_text, "field_request_sha256"),
        _rou_sha256_string(network_hash_text, "network_sha256"),
        coordinates,
        parameters,
        schedule,
        solver,
        algorithm,
    )
end

function ro_scientific_source_identity_payload(source::ROScientificSourceIdentity)
    return (
        schema_version=RO_SCIENTIFIC_SOURCE_IDENTITY_VERSION,
        source_field_sha256=source.source_field_sha256,
        field_request_sha256=source.field_request_sha256,
        network_sha256=source.network_sha256,
        local_coordinates=source.local_coordinates,
        nominal_parameters=source.nominal_parameters,
        observation_schedule=source.observation_schedule,
        solver_revision=source.solver_revision,
        algorithm_revision=source.algorithm_revision,
    )
end

_rou_sha256(payload) = bytes2hex(SHA.sha256(codeunits(JSON3.write(payload))))

ro_scientific_source_identity_sha256(source::ROScientificSourceIdentity) =
    _rou_sha256(ro_scientific_source_identity_payload(source))

function _rou_rebuild_scientific_source(
    source::ROScientificSourceIdentity;
    cancel_check=()->nothing,
    limits::ROUncertaintyLimits=ROUncertaintyLimits(),
)
    rebuild_cancel_check = _rou_cancel_after_entry(cancel_check)
    return ROScientificSourceIdentity(
        source_field_sha256=source.source_field_sha256,
        field_request_sha256=source.field_request_sha256,
        network_sha256=source.network_sha256,
        local_coordinates=source.local_coordinates,
        nominal_parameters=source.nominal_parameters,
        observation_schedule=source.observation_schedule,
        solver_revision=source.solver_revision,
        algorithm_revision=source.algorithm_revision,
        cancel_check=rebuild_cancel_check,
        limits=limits,
    )
end

"""Separated uncertainty classes and typed invalid-gap population."""
struct ROUncertaintyPartition
    parametric_uncertainty::Symbol
    experimental_uncertainty::Symbol
    numerical_uncertainty::Symbol
    valid_indices::Tuple{Vararg{Int}}
    invalid_gap_indices::Tuple{Vararg{Int}}
    invalid_gap_reasons::Tuple{Vararg{Symbol}}
    structural_uncertainty::Symbol
    structural_ambiguity_reasons::Tuple{Vararg{Symbol}}

    function ROUncertaintyPartition(::_ROUConstructionToken, args...)
        return new(args...)
    end
end

"""Local SVD/Fisher-information evidence under one declared model."""
struct ROLocalIdentifiabilityAnalysis
    schema_version::String
    identity::ROFieldEvidenceIdentity
    source::ROScientificSourceIdentity
    identity_sha256::String
    identity_payload::NamedTuple
    uncertainty::ROUncertaintyPartition
    status::Symbol
    fim::Union{Nothing,Matrix{Float64}}
    fim_singular_values::Vector{Float64}
    whitened_sensitivity_singular_values::Vector{Float64}
    rank_lower_bound::Int
    rank_upper_bound::Int
    numerical_rank::Union{Nothing,Int}
    requested_rank_threshold_low::Union{Nothing,Float64}
    requested_rank_threshold_high::Union{Nothing,Float64}
    rank_threshold_low::Union{Nothing,Float64}
    rank_threshold_high::Union{Nothing,Float64}
    backward_error_floor::Union{Nothing,Float64}
    backward_error_multiplier::Float64
    rank_threshold_floor_dominated::Bool
    rank_method::Symbol
    rank_status::Symbol
    condition_number::Union{Nothing,Float64}
    condition_status::Symbol
    structural_local_identifiability::Symbol
    practical_precision_status::Symbol
    practical_parameter_covariance::Union{Nothing,Matrix{Float64}}
    practical_parameter_standard_errors::Union{Nothing,Vector{Float64}}
    evidence_scope::Symbol
    global_identifiability_claimed::Bool
    causal_claimed::Bool
    experimentally_validated::Bool

    function ROLocalIdentifiabilityAnalysis(
        ::_ROUConstructionToken,
        args...,
    )
        owned = Any[args...]
        for index in (8, 9, 10, 27, 28)
            owned[index] = owned[index] === nothing ? nothing :
                copy(owned[index])
        end
        return new(owned...)
    end
end

"""First-order covariance propagation with explicit invalid output gaps."""
struct RODeltaMethodCovariance
    schema_version::String
    identity::ROFieldEvidenceIdentity
    source::ROScientificSourceIdentity
    identity_sha256::String
    identity_payload::NamedTuple
    uncertainty::ROUncertaintyPartition
    status::Symbol
    parameter_covariance_factor::Matrix{Float64}
    output_covariance_factor::Matrix{Float64}
    output_covariance::Matrix{Float64}
    output_standard_deviations::Vector{Float64}
    valid_output_count::Int
    invalid_output_count::Int
    covariance_psd_tolerance::Float64
    output_psd_status::Symbol
    output_covariance_minimum_eigenvalue::Union{Nothing,Float64}
    output_covariance_backward_error_floor::Union{Nothing,Float64}
    propagation_method::Symbol
    method_scope::Symbol
    calibration_status::Symbol
    causal_claimed::Bool
    experimentally_validated::Bool

    function RODeltaMethodCovariance(::_ROUConstructionToken, args...)
        owned = Any[args...]
        for index in (8, 9, 10, 11)
            owned[index] = copy(owned[index])
        end
        return new(owned...)
    end
end

"""Computed synthetic coverage fixture; never an experimental calibration."""
struct ROSyntheticCoverageEvidence
    schema_version::String
    fixture_id::String
    source_fixture_sha256::String
    feature_ids::Tuple{Vararg{String}}
    identity_sha256::String
    identity_payload::NamedTuple
    expected_case_count::Int
    valid_case_count::Int
    invalid_case_count::Int
    invalid_case_ids::Tuple{Vararg{String}}
    invalid_gap_reasons::Tuple{Vararg{Symbol}}
    feature_coverage_counts::Vector{Int}
    feature_valid_counts::Vector{Int}
    feature_coverage::Vector{Union{Nothing,Float64}}
    joint_coverage_count::Int
    joint_valid_count::Int
    joint_coverage::Union{Nothing,Float64}
    status::Symbol
    calibration_status::Symbol
    experimentally_calibrated::Bool

    function ROSyntheticCoverageEvidence(::_ROUConstructionToken, args...)
        owned = Any[args...]
        for index in (12, 13, 14)
            owned[index] = copy(owned[index])
        end
        return new(owned...)
    end
end

"""The sole owner of uncertainty class and declared replicate population."""
abstract type ROPopulationPolicy end

function _rou_reject_reserved_policy_keys(
    specification::NamedTuple,
    reserved,
    label::AbstractString,
)
    collisions = Tuple(key for key in keys(specification) if key in reserved)
    isempty(collisions) || throw(ArgumentError(
        "$label must not redeclare typed owner fields: $(collisions)"))
    return nothing
end

function _rou_reject_reserved_policy_names(
    names::Tuple,
    reserved,
    label::AbstractString,
)
    all(name -> name isa String, names) || throw(ArgumentError(
        "$label canonical entry names must be Strings"))
    collisions = Tuple(name for name in names if
        any(key -> name == String(key), reserved))
    isempty(collisions) || throw(ArgumentError(
        "$label must not redeclare typed owner fields: $(collisions)"))
    return nothing
end

"""A complete finite draw population from one declared distribution."""
struct ROEnsemblePopulationPolicy <: ROPopulationPolicy
    uncertainty_class::Symbol
    policy_id::String
    policy_revision::String
    draw_count::Int
    distribution_family::Symbol
    distribution_specification::NamedTuple
    sampling_revision::String

    function ROEnsemblePopulationPolicy(
        ::_ROUConstructionToken,
        uncertainty_class::Symbol,
        policy_id::String,
        policy_revision::String,
        draw_count::Int,
        distribution_family::Symbol,
        distribution_specification::NamedTuple,
        sampling_revision::String,
        limits::ROUncertaintyLimits,
        cancel_check,
    )
        uncertainty_class in (:parametric, :experimental) ||
            throw(ArgumentError(
                "uncertainty_class must be :parametric or :experimental"))
        id = _rou_string_tuple([policy_id], "policy_id")[1]
        revision = _rou_string_tuple(
            [policy_revision], "policy_revision")[1]
        draw_count > 0 || throw(ArgumentError("draw_count must be positive"))
        _rou_limit(
            :replicate_population, BigInt(draw_count), limits.max_replicates)
        (sizeof(distribution_family) == 0 ||
            distribution_family in (:none, :unknown)) && throw(ArgumentError(
            "distribution_family must name a concrete distribution"))
        _rou_reject_reserved_policy_keys(
            distribution_specification,
            (
                :uncertainty_class,
                :policy_id,
                :policy_revision,
                :draw_count,
                :population_count,
                :distribution_family,
                :sampling_revision,
                :enumeration,
                :interval_semantics,
                :coordinate_ids,
                :coordinate_units,
                :coordinate_lower,
                :coordinate_upper,
                :interval_definition_sha256,
                :interval_certificate_sha256,
            ),
            "distribution_specification",
        )
        specification = _rou_canonical_policy_value(
            distribution_specification; limits, cancel_check)
        sampling = _rou_string_tuple(
            [sampling_revision], "sampling_revision")[1]
        return new(
            uncertainty_class,
            id,
            revision,
            draw_count,
            distribution_family,
            specification,
            sampling,
        )
    end
end

function ROEnsemblePopulationPolicy(;
    uncertainty_class,
    policy_id,
    policy_revision,
    draw_count,
    distribution_family,
    distribution_specification,
    sampling_revision,
    cancel_check=()->nothing,
    limits::ROUncertaintyLimits=ROUncertaintyLimits(),
)
    _rou_cancel(cancel_check)
    limits = _rou_rebuild_limits(limits)
    uncertainty_class isa Symbol || throw(ArgumentError(
        "uncertainty_class must be a Symbol"))
    distribution_family isa Symbol || throw(ArgumentError(
        "distribution_family must be a Symbol"))
    distribution_specification isa NamedTuple || throw(ArgumentError(
        "distribution_specification must be a NamedTuple"))
    draw_count isa Integer && !(draw_count isa Bool) || throw(ArgumentError(
        "draw_count must be an integer"))
    draw_count <= typemax(Int) || throw(ArgumentError(
        "draw_count must fit in Int"))
    draw_count > 0 || throw(ArgumentError("draw_count must be positive"))
    _rou_limit(
        :replicate_population, BigInt(draw_count), limits.max_replicates)
    byte_count = Ref(BigInt(0))
    id = _rou_bounded_string(
        policy_id, "policy_id", byte_count, limits,
        :policy_metadata_bytes; cancel_check)
    revision = _rou_bounded_string(
        policy_revision, "policy_revision", byte_count, limits,
        :policy_metadata_bytes; cancel_check)
    sampling = _rou_bounded_string(
        sampling_revision, "sampling_revision", byte_count, limits,
        :policy_metadata_bytes; cancel_check)
    _rou_bounded_symbol_string(
        distribution_family, "distribution_family", byte_count,
        limits, :policy_metadata_bytes; cancel_check)
    return ROEnsemblePopulationPolicy(
        _ROU_CONSTRUCTION_TOKEN,
        uncertainty_class,
        id,
        revision,
        Int(draw_count),
        distribution_family,
        distribution_specification,
        sampling,
        limits,
        cancel_check,
    )
end

function _rou_interval_definition_payload(
    population_count,
    enumeration,
    interval_semantics,
    coordinate_ids,
    coordinate_units,
    coordinate_lower,
    coordinate_upper,
    interval_specification,
)
    return (
        schema_version=RO_INTERVAL_COORDINATE_DEFINITION_VERSION,
        population_count=population_count,
        enumeration=enumeration,
        interval_semantics=interval_semantics,
        coordinate_ids=coordinate_ids,
        coordinate_units=coordinate_units,
        coordinate_lower=coordinate_lower,
        coordinate_upper=coordinate_upper,
        interval_specification=interval_specification,
    )
end

"""A typed, explicitly enumerated finite coordinate population."""
struct ROCertifiedIntervalPopulationPolicy <: ROPopulationPolicy
    uncertainty_class::Symbol
    policy_id::String
    policy_revision::String
    population_count::Int
    enumeration::Symbol
    interval_semantics::Symbol
    coordinate_ids::Tuple{Vararg{String}}
    coordinate_units::Tuple{Vararg{String}}
    coordinate_lower::Tuple{Vararg{Float64}}
    coordinate_upper::Tuple{Vararg{Float64}}
    interval_definition_sha256::String
    interval_certificate_sha256::String
    interval_specification::NamedTuple

    function ROCertifiedIntervalPopulationPolicy(
        ::_ROUConstructionToken,
        uncertainty_class::Symbol,
        policy_id::String,
        policy_revision::String,
        population_count::Int,
        enumeration::Symbol,
        interval_semantics::Symbol,
        coordinate_ids,
        coordinate_units,
        coordinate_lower,
        coordinate_upper,
        interval_definition_sha256,
        interval_certificate_sha256::String,
        interval_specification::NamedTuple,
        byte_count::Base.RefValue{BigInt},
        limits::ROUncertaintyLimits,
        cancel_check,
    )
        uncertainty_class in (:parametric, :experimental) ||
            throw(ArgumentError(
                "uncertainty_class must be :parametric or :experimental"))
        id = _rou_string_tuple([policy_id], "policy_id")[1]
        revision = _rou_string_tuple(
            [policy_revision], "policy_revision")[1]
        population_count > 0 || throw(ArgumentError(
            "population_count must be positive"))
        _rou_limit(
            :replicate_population,
            BigInt(population_count),
            limits.max_replicates,
        )
        enumeration == :explicit_complete_coordinate_population ||
            throw(ArgumentError(
                "enumeration must be :explicit_complete_coordinate_population"))
        (sizeof(interval_semantics) == 0 ||
            interval_semantics in (:none, :unknown)) && throw(ArgumentError(
            "interval_semantics must name a concrete finite population"))
        coordinate_limit = uncertainty_class == :parametric ?
            limits.max_parameters : limits.max_observations
        coordinate_count = _rou_preflight_sized_collection(
            coordinate_ids, "coordinate_ids", :replicate_coordinate_dimensions,
            coordinate_limit)
        coordinate_count > 0 || throw(ArgumentError(
            "coordinate_ids must contain at least one coordinate"))
        _rou_preflight_sized_collection(
            coordinate_units, "coordinate_units",
            :replicate_coordinate_dimensions, coordinate_limit) ==
            coordinate_count || throw(DimensionMismatch(
            "coordinate_units must match coordinate_ids"))
        _rou_preflight_sized_collection(
            coordinate_lower, "coordinate_lower",
            :replicate_coordinate_dimensions, coordinate_limit) ==
            coordinate_count || throw(DimensionMismatch(
            "coordinate_lower must match coordinate_ids"))
        _rou_preflight_sized_collection(
            coordinate_upper, "coordinate_upper",
            :replicate_coordinate_dimensions, coordinate_limit) ==
            coordinate_count || throw(DimensionMismatch(
            "coordinate_upper must match coordinate_ids"))
        _rou_charge_metadata_bytes!(
            byte_count, 16 * BigInt(coordinate_count), limits,
            :policy_metadata_bytes)
        ids = _rou_bounded_string_tuple(
            coordinate_ids, coordinate_count, "coordinate_ids", byte_count,
            limits, :policy_metadata_bytes; unique_names=true, cancel_check)
        units = _rou_bounded_string_tuple(
            coordinate_units, coordinate_count, "coordinate_units",
            byte_count, limits, :policy_metadata_bytes; cancel_check)
        lower = _rou_bounded_finite_float_tuple(
            coordinate_lower, coordinate_count, "coordinate_lower";
            cancel_check)
        upper = _rou_bounded_finite_float_tuple(
            coordinate_upper, coordinate_count, "coordinate_upper";
            cancel_check)
        all(lower[index] <= upper[index] for index in eachindex(lower)) ||
            throw(ArgumentError(
                "coordinate lower bounds must not exceed upper bounds"))
        certificate_hash = _rou_sha256_string(
            interval_certificate_sha256, "interval_certificate_sha256")
        _rou_reject_reserved_policy_keys(
            interval_specification,
            (
                :uncertainty_class,
                :policy_id,
                :policy_revision,
                :population_count,
                :draw_count,
                :distribution_family,
                :sampling_revision,
                :enumeration,
                :interval_semantics,
                :coordinate_ids,
                :coordinate_units,
                :coordinate_lower,
                :coordinate_upper,
                :lower,
                :upper,
                :interval_definition_sha256,
                :interval_certificate_sha256,
            ),
            "interval_specification",
        )
        specification = _rou_canonical_policy_value(
            interval_specification; limits, cancel_check)
        definition_payload = _rou_interval_definition_payload(
            population_count,
            enumeration,
            interval_semantics,
            ids,
            units,
            lower,
            upper,
            specification,
        )
        definition_hash = _rou_sha256(definition_payload)
        if interval_definition_sha256 !== nothing
            supplied_definition_hash = _rou_sha256_string(
                interval_definition_sha256,
                "interval_definition_sha256",
            )
            supplied_definition_hash == definition_hash ||
                throw(ArgumentError(
                    "interval_definition_sha256 does not match typed content"))
        end
        return new(
            uncertainty_class,
            id,
            revision,
            population_count,
            enumeration,
            interval_semantics,
            ids,
            units,
            lower,
            upper,
            definition_hash,
            certificate_hash,
            specification,
        )
    end
end


function ROCertifiedIntervalPopulationPolicy(;
    uncertainty_class,
    policy_id,
    policy_revision,
    population_count,
    enumeration=:explicit_complete_coordinate_population,
    interval_semantics,
    coordinate_ids,
    coordinate_units,
    coordinate_lower,
    coordinate_upper,
    interval_definition_sha256=nothing,
    interval_certificate_sha256,
    interval_specification,
    cancel_check=()->nothing,
    limits::ROUncertaintyLimits=ROUncertaintyLimits(),
)
    _rou_cancel(cancel_check)
    limits = _rou_rebuild_limits(limits)
    uncertainty_class isa Symbol || throw(ArgumentError(
        "uncertainty_class must be a Symbol"))
    enumeration isa Symbol || throw(ArgumentError(
        "enumeration must be a Symbol"))
    interval_semantics isa Symbol || throw(ArgumentError(
        "interval_semantics must be a Symbol"))
    interval_specification isa NamedTuple || throw(ArgumentError(
        "interval_specification must be a NamedTuple"))
    population_count isa Integer && !(population_count isa Bool) ||
        throw(ArgumentError("population_count must be an integer"))
    population_count <= typemax(Int) || throw(ArgumentError(
        "population_count must fit in Int"))
    population_count > 0 || throw(ArgumentError(
        "population_count must be positive"))
    _rou_limit(
        :replicate_population,
        BigInt(population_count),
        limits.max_replicates,
    )
    coordinate_limit = uncertainty_class == :parametric ?
        limits.max_parameters : limits.max_observations
    coordinate_count = _rou_preflight_sized_collection(
        coordinate_ids, "coordinate_ids", :replicate_coordinate_dimensions,
        coordinate_limit)
    _rou_preflight_sized_collection(
        coordinate_units, "coordinate_units",
        :replicate_coordinate_dimensions, coordinate_limit) ==
        coordinate_count || throw(DimensionMismatch(
            "coordinate_units must match coordinate_ids"))
    _rou_preflight_sized_collection(
        coordinate_lower, "coordinate_lower",
        :replicate_coordinate_dimensions, coordinate_limit) ==
        coordinate_count || throw(DimensionMismatch(
            "coordinate_lower must match coordinate_ids"))
    _rou_preflight_sized_collection(
        coordinate_upper, "coordinate_upper",
        :replicate_coordinate_dimensions, coordinate_limit) ==
        coordinate_count || throw(DimensionMismatch(
            "coordinate_upper must match coordinate_ids"))
    byte_count = Ref(BigInt(0))
    id = _rou_bounded_string(
        policy_id, "policy_id", byte_count, limits,
        :policy_metadata_bytes; cancel_check)
    revision = _rou_bounded_string(
        policy_revision, "policy_revision", byte_count, limits,
        :policy_metadata_bytes; cancel_check)
    certificate_hash = _rou_bounded_string(
        interval_certificate_sha256, "interval_certificate_sha256",
        byte_count, limits, :policy_metadata_bytes; cancel_check)
    definition_hash = interval_definition_sha256 === nothing ? nothing :
        _rou_bounded_string(
            interval_definition_sha256, "interval_definition_sha256",
            byte_count, limits, :policy_metadata_bytes; cancel_check)
    _rou_bounded_symbol_string(
        interval_semantics, "interval_semantics", byte_count, limits,
        :policy_metadata_bytes; cancel_check)
    return ROCertifiedIntervalPopulationPolicy(
        _ROU_CONSTRUCTION_TOKEN,
        uncertainty_class,
        id,
        revision,
        Int(population_count),
        enumeration,
        interval_semantics,
        coordinate_ids,
        coordinate_units,
        coordinate_lower,
        coordinate_upper,
        definition_hash,
        certificate_hash,
        interval_specification,
        byte_count,
        limits,
        cancel_check,
    )
end

"""Complete declared ensemble or certified-interval population artifact."""
struct ROUncertaintyPopulationArtifact
    schema_version::String
    identity::ROFieldEvidenceIdentity
    source::ROScientificSourceIdentity
    identity_sha256::String
    identity_payload::NamedTuple
    uncertainty::ROUncertaintyPartition
    status::Symbol
    uncertainty_class::Symbol
    policy_kind::Symbol
    policy_id::String
    policy_revision::String
    expected_population_count::Int
    population_ids::Tuple{Vararg{String}}
    replicate_coordinate_ids::Tuple{Vararg{String}}
    replicate_coordinates::Matrix{Float64}
    valid_replicate_count::Int
    invalid_replicate_count::Int
    gap_probability::Float64
    quantile_probabilities::Tuple{Vararg{Float64}}
    feature_quantiles::Union{Nothing,Matrix{Float64}}
    quantile_scope::Symbol
    certified_bounds::Union{Nothing,Matrix{Float64}}
    interval_certificate_sha256::Union{Nothing,String}
    bounds_status::Symbol
    calibration_evidence::Union{Nothing,ROSyntheticCoverageEvidence}
    calibration_status::Symbol
    evidence_scope::Symbol
    causal_claimed::Bool
    global_robustness_claimed::Bool
    experimentally_validated::Bool

    function ROUncertaintyPopulationArtifact(
        ::_ROUConstructionToken,
        args...,
    )
        owned = Any[args...]
        for index in (15, 20, 22)
            owned[index] = owned[index] === nothing ? nothing :
                copy(owned[index])
        end
        owned[25] = owned[25] === nothing ? nothing : deepcopy(owned[25])
        return new(owned...)
    end
end

function _rou_float_matrix(
    values::AbstractMatrix,
    label::AbstractString;
    cancel_check=()->nothing,
)
    eltype(values) === Float64 || throw(ArgumentError(
        "$label must use Float64 elements exactly"))
    result = Matrix{Float64}(undef, size(values))
    for (position, index) in enumerate(eachindex(values))
        result[index] = _rou_canonical_float(values[index])
        position % 4_096 == 0 && _rou_cancel(cancel_check)
    end
    return result
end

function _rou_float_vector(
    values::AbstractVector,
    label::AbstractString;
    cancel_check=()->nothing,
)
    eltype(values) === Float64 || throw(ArgumentError(
        "$label must use Float64 elements exactly"))
    result = Vector{Float64}(undef, length(values))
    for (position, index) in enumerate(eachindex(values))
        result[position] = _rou_canonical_float(values[index])
        position % 4_096 == 0 && _rou_cancel(cancel_check)
    end
    return result
end

function _rou_validity(
    values,
    count::Int,
    label::AbstractString;
    cancel_check=()->nothing,
)
    values === nothing && return trues(count)
    values isa AbstractVector{Bool} || throw(ArgumentError(
        "$label must be a Boolean vector"))
    length(values) == count || throw(DimensionMismatch(
        "$label length must be $count"))
    result = falses(count)
    for index in eachindex(values)
        result[index] = values[index]
        Int(index) % 4_096 == 0 && _rou_cancel(cancel_check)
    end
    return result
end

function _rou_gap_reasons(
    validity::BitVector,
    values,
    label::AbstractString;
    cancel_check=()->nothing,
)
    if values === nothing
        all(validity) || throw(ArgumentError(
            "$label must type every invalid gap"))
        return ntuple(_ -> nothing, length(validity))
    end
    length(values) == length(validity) || throw(DimensionMismatch(
        "$label length must match validity"))
    result = Vector{Union{Nothing,Symbol}}(undef, length(validity))
    for index in eachindex(validity)
        value = values[index]
        if validity[index]
            value === nothing || throw(ArgumentError(
                "$label[$index] must be nothing for a valid value"))
            result[index] = nothing
        else
            value isa Symbol || throw(ArgumentError(
                "$label[$index] must be a Symbol for an invalid gap"))
            value in (:none, :valid) && throw(ArgumentError(
                "$label[$index] must be a meaningful invalid-gap reason"))
            result[index] = value
        end
        index % 4_096 == 0 && _rou_cancel(cancel_check)
    end
    return Tuple(result)
end

function _rou_structural_reasons(
    values;
    cancel_check=()->nothing,
    limits::ROUncertaintyLimits=ROUncertaintyLimits(),
)
    limits = _rou_rebuild_limits(limits)
    _rou_preflight_sized_collection(
        values, "structural_ambiguity_reasons", :structural_reasons,
        limits.max_policy_elements)
    reasons = Symbol[]
    for (position, value) in enumerate(values)
        value isa Symbol || throw(ArgumentError(
            "structural ambiguity reasons must be Symbols"))
        value in (:none, :valid) && throw(ArgumentError(
            "structural ambiguity reasons must be meaningful"))
        push!(reasons, value)
        position % 1_024 == 0 && _rou_cancel(cancel_check)
    end
    return Tuple(sort!(unique!(reasons); by=string))
end

function _rou_validate_identity_dimensions(
    identity::ROFieldEvidenceIdentity,
    source::ROScientificSourceIdentity,
    observation_count::Int,
    parameter_count::Int,
    limits::ROUncertaintyLimits,
)
    length(identity.input_order) <= limits.max_inputs ||
        throw(ROUncertaintyLimitExceeded(
            :input_dimensions,
            BigInt(length(identity.input_order)),
            limits.max_inputs,
        ))
    parameter_count <= limits.max_parameters ||
        throw(ROUncertaintyLimitExceeded(
            :parameter_dimensions, BigInt(parameter_count), limits.max_parameters))
    observation_count <= limits.max_observations ||
        throw(ROUncertaintyLimitExceeded(
            :observation_dimensions,
            BigInt(observation_count),
            limits.max_observations,
        ))
    observation_count <= limits.max_outputs ||
        throw(ROUncertaintyLimitExceeded(
            :output_dimensions, BigInt(observation_count), limits.max_outputs))
    length(identity.parameter_order) == parameter_count ||
        throw(DimensionMismatch(
            "sensitivity/Jacobian columns must match parameter_order"))
    length(identity.output_order) == observation_count ||
        throw(DimensionMismatch(
            "sensitivity/Jacobian rows must match output_order"))
    length(source.local_coordinates) == length(identity.input_order) ||
        throw(DimensionMismatch(
            "source local_coordinates must match input_order"))
    length(source.nominal_parameters) == parameter_count ||
        throw(DimensionMismatch(
            "source nominal_parameters must match parameter_order"))
    length(source.observation_schedule) == observation_count ||
        throw(DimensionMismatch(
            "source observation_schedule must match output_order"))
    return nothing
end

function _rou_preflight_identity_source_dimensions(
    identity::ROFieldEvidenceIdentity,
    source::ROScientificSourceIdentity,
    observation_count::Int,
    parameter_count::Int,
    limits::ROUncertaintyLimits,
)
    input_count = length(identity.input_order)
    declared_parameter_count = length(identity.parameter_order)
    declared_output_count = length(identity.output_order)
    input_count <= limits.max_inputs || throw(ROUncertaintyLimitExceeded(
        :input_dimensions, BigInt(input_count), limits.max_inputs))
    parameter_count <= limits.max_parameters ||
        throw(ROUncertaintyLimitExceeded(
            :parameter_dimensions, BigInt(parameter_count),
            limits.max_parameters))
    observation_count <= limits.max_observations ||
        throw(ROUncertaintyLimitExceeded(
            :observation_dimensions, BigInt(observation_count),
            limits.max_observations))
    observation_count <= limits.max_outputs ||
        throw(ROUncertaintyLimitExceeded(
            :output_dimensions, BigInt(observation_count), limits.max_outputs))
    declared_parameter_count == parameter_count || throw(DimensionMismatch(
        "declared parameter count does not match numeric input"))
    declared_output_count == observation_count || throw(DimensionMismatch(
        "declared output count does not match numeric input"))
    length(identity.input_units) == input_count &&
        length(identity.input_scales) == input_count ||
        throw(DimensionMismatch("input metadata dimensions are invalid"))
    length(identity.parameter_units) == parameter_count &&
        length(identity.parameter_scales) == parameter_count ||
        throw(DimensionMismatch("parameter metadata dimensions are invalid"))
    length(identity.output_units) == observation_count &&
        length(identity.output_scales) == observation_count ||
        throw(DimensionMismatch("output metadata dimensions are invalid"))
    length(source.local_coordinates) == input_count ||
        throw(DimensionMismatch("source coordinates do not match inputs"))
    length(source.nominal_parameters) == parameter_count ||
        throw(DimensionMismatch("source parameters do not match parameters"))
    length(source.observation_schedule) == observation_count ||
        throw(DimensionMismatch("source schedule does not match outputs"))
    return nothing
end

function _rou_matrix_payload(
    matrix::AbstractMatrix{<:Real};
    cancel_check=()->nothing,
)
    rows = Any[]
    sizehint!(rows, size(matrix, 1))
    position = 0
    for row in axes(matrix, 1)
        values = Float64[]
        sizehint!(values, size(matrix, 2))
        for column in axes(matrix, 2)
            push!(values, _rou_canonical_float(matrix[row, column]))
            position += 1
            position % 4_096 == 0 && _rou_cancel(cancel_check)
        end
        push!(rows, Tuple(values))
    end
    return Tuple(rows)
end

_rou_optional_matrix_payload(matrix; cancel_check=()->nothing) =
    matrix === nothing ? nothing :
        _rou_matrix_payload(matrix; cancel_check)

function _rou_optional_vector_payload(values; cancel_check=()->nothing)
    values === nothing && return nothing
    result = Float64[]
    sizehint!(result, length(values))
    for (position, value) in enumerate(values)
        push!(result, _rou_canonical_float(value))
        position % 4_096 == 0 && _rou_cancel(cancel_check)
    end
    return Tuple(result)
end

function _rou_valid_vector_payload(
    values,
    validity::BitVector;
    cancel_check=()->nothing,
)
    result = Any[]
    sizehint!(result, length(validity))
    for index in eachindex(validity)
        push!(result, validity[index] ?
            _rou_canonical_float(values[index]) : nothing)
        index % 4_096 == 0 && _rou_cancel(cancel_check)
    end
    return Tuple(result)
end

function _rou_valid_submatrix_payload(
    matrix::AbstractMatrix{<:Real},
    validity::BitVector,
    ;
    cancel_check=()->nothing,
)
    indices = findall(validity)
    values = Matrix{Float64}(undef, length(indices), length(indices))
    for (local_row, row) in enumerate(indices)
        for (local_column, column) in enumerate(indices)
            values[local_row, local_column] = matrix[row, column]
        end
        _rou_cancel(cancel_check)
    end
    return _rou_matrix_payload(values; cancel_check)
end

function _rou_valid_rows_payload(
    matrix::Matrix{Float64},
    validity::BitVector;
    cancel_check=()->nothing,
)
    rows = Any[]
    sizehint!(rows, size(matrix, 1))
    position = 0
    for row in axes(matrix, 1)
        if validity[row]
            values = Float64[]
            sizehint!(values, size(matrix, 2))
            for column in axes(matrix, 2)
                push!(values, _rou_canonical_float(matrix[row, column]))
                position += 1
                position % 4_096 == 0 && _rou_cancel(cancel_check)
            end
            push!(rows, Tuple(values))
        else
            push!(rows, nothing)
        end
    end
    return Tuple(rows)
end

function _rou_relative_tolerance(value::Real, label::AbstractString)
    tolerance = _rou_canonical_float(value)
    isfinite(tolerance) && tolerance >= 0 || throw(ArgumentError(
        "$label must be finite and nonnegative"))
    return tolerance
end

@inline function _rou_matrix_scale(matrix::AbstractMatrix{Float64})
    scale = maximum(abs, matrix; init=0.0)
    return scale == 0.0 ? floatmin(Float64) : scale
end

function _rou_symmetric_matrix(
    matrix::Matrix{Float64},
    label::AbstractString,
    symmetry_tolerance::Float64,
)
    all(isfinite, matrix) || throw(ArgumentError("$label must be finite"))
    scale = _rou_matrix_scale(matrix)
    residual = maximum(abs, matrix - transpose(matrix); init=0.0)
    backward_floor = 8 * eps(Float64) * max(size(matrix)...) * scale
    admission = max(symmetry_tolerance * scale, backward_floor)
    residual <= admission || throw(ArgumentError(
        "$label must be symmetric within the scale-relative admission bound"))
    return Matrix(Symmetric((matrix + transpose(matrix)) / 2)),
        residual, admission, scale
end

function _rou_psd_eigen(
    covariance::Matrix{Float64},
    label::AbstractString,
    psd_tolerance::Float64;
    cancel_check,
)
    _rou_cancel(cancel_check)
    decomposition = eigen(Symmetric(covariance))
    _rou_cancel(cancel_check)
    spectrum = decomposition.values
    all(isfinite, spectrum) || throw(ArgumentError(
        "$label has a non-finite eigenvalue"))
    scale = max(_rou_matrix_scale(covariance), maximum(abs, spectrum; init=0.0))
    backward_floor = 32 * eps(Float64) * max(1, size(covariance, 1)) * scale
    ambiguity_floor = max(psd_tolerance * scale, backward_floor)
    minimum_value = minimum(spectrum; init=0.0)
    minimum_value < -ambiguity_floor && throw(ArgumentError(
        "$label must be positive semidefinite"))
    minimum_value < 0.0 && throw(ROCovarianceNumericallyAmbiguous(
        String(label), minimum_value, ambiguity_floor))
    return decomposition, scale, backward_floor, ambiguity_floor
end

function _rou_common_status(validity::BitVector, structural_reasons)
    has_gaps = !all(validity)
    has_structure = !isempty(structural_reasons)
    return has_gaps && has_structure ? :unknown_multiple_evidence_gaps :
        has_gaps ? :unknown_numerical_gap :
        has_structure ? :unknown_structural_ambiguity : :complete
end

function _rou_partition(
    validity::BitVector,
    gap_reasons,
    structural_reasons;
    parametric::Symbol,
    experimental::Symbol,
)
    valid = Tuple(findall(validity))
    invalid = Tuple(findall(.!validity))
    invalid_reasons = Tuple(gap_reasons[index]::Symbol for index in invalid)
    return ROUncertaintyPartition(
        _ROU_CONSTRUCTION_TOKEN,
        parametric,
        experimental,
        isempty(invalid) ? :complete : :explicit_typed_invalid_gaps,
        valid,
        invalid,
        invalid_reasons,
        isempty(structural_reasons) ? :none_declared : :declared_ambiguity,
        structural_reasons,
    )
end

function _rou_rank_thresholds(
    singular_scale::Float64,
    row_count::Int,
    parameter_count::Int,
    absolute_low::Float64,
    absolute_high::Float64,
    relative_low::Float64,
    relative_high::Float64,
    backward_multiplier::Float64,
)
    requested_low = max(absolute_low, relative_low * singular_scale)
    requested_high = max(absolute_high, relative_high * singular_scale)
    backward_floor = backward_multiplier * eps(Float64) *
        max(row_count, parameter_count) * singular_scale
    all(isfinite, (requested_low, requested_high, backward_floor)) ||
        throw(ArgumentError("effective rank thresholds must remain finite"))
    effective_low = max(requested_low, backward_floor)
    effective_high = max(requested_high, 2 * backward_floor)
    effective_low < effective_high || throw(ArgumentError(
        "effective low/high numerical-rank thresholds must remain distinct"))
    floor_dominated = effective_low > requested_low ||
        effective_high > requested_high
    return requested_low, requested_high, effective_low, effective_high,
        backward_floor, floor_dominated
end

function _rou_local_identity_payload(
    identity,
    source,
    sensitivity,
    validity,
    gap_reasons,
    structural_reasons,
    observation_model,
    absolute_low,
    absolute_high,
    relative_low,
    relative_high,
    requested_low,
    requested_high,
    effective_low,
    effective_high,
    backward_floor,
    backward_multiplier,
    floor_dominated,
    ;
    cancel_check=()->nothing,
)
    return (
        schema_version=RO_LOCAL_IDENTIFIABILITY_VERSION,
        coordinate_identity=ro_field_evidence_identity_payload(identity),
        scientific_source=ro_scientific_source_identity_payload(source),
        scientific_source_sha256=ro_scientific_source_identity_sha256(source),
        observation_validity=Tuple(validity),
        observation_gap_reasons=gap_reasons,
        sensitivity_rows=_rou_valid_rows_payload(
            sensitivity, validity; cancel_check),
        observation_model=observation_model,
        structural_ambiguity_reasons=structural_reasons,
        numerical_rank_policy=(
            method=:whitened_sensitivity_svd,
            absolute_low=absolute_low,
            absolute_high=absolute_high,
            relative_low=relative_low,
            relative_high=relative_high,
            requested_low=requested_low,
            requested_high=requested_high,
            effective_low=effective_low,
            effective_high=effective_high,
            backward_error_floor=backward_floor,
            backward_error_multiplier=backward_multiplier,
            backward_error_floor_dominated=floor_dominated,
        ),
        evidence_scope=RO_LOCAL_IDENTIFIABILITY_SCOPE,
    )
end

function _rou_local_result_snapshot(;
    status,
    fim,
    fim_singular_values,
    whitened_singular_values,
    rank_lower_bound,
    rank_upper_bound,
    numerical_rank,
    rank_status,
    condition_number,
    condition_status,
    structural_status,
    practical_status,
    practical_covariance,
    practical_standard_errors,
    cancel_check=()->nothing,
)
    return (
        status=status,
        fim=_rou_optional_matrix_payload(fim; cancel_check),
        fim_singular_values=Tuple(fim_singular_values),
        whitened_sensitivity_singular_values=Tuple(whitened_singular_values),
        rank_lower_bound=rank_lower_bound,
        rank_upper_bound=rank_upper_bound,
        numerical_rank=numerical_rank,
        rank_status=rank_status,
        condition_number=condition_number,
        condition_status=condition_status,
        structural_local_identifiability=structural_status,
        practical_precision_status=practical_status,
        practical_parameter_covariance=
            _rou_optional_matrix_payload(
                practical_covariance; cancel_check),
        practical_parameter_standard_errors=
            _rou_optional_vector_payload(
                practical_standard_errors; cancel_check),
    )
end


"""
    analyze_ro_local_identifiability(sensitivity; identity, source, ...)

Whiten the valid local sensitivity rows and determine numerical rank directly
from their SVD. Requested thresholds are raised to an explicit SVD backward-
error floor; both requested and effective grey zones are retained in identity.
Full rank remains only local evidence under the declared source, schedule,
solver, algorithm, and observation model.
"""
function analyze_ro_local_identifiability(
    sensitivity_values::AbstractMatrix{<:Real};
    identity::ROFieldEvidenceIdentity,
    source::ROScientificSourceIdentity,
    observation_covariance=nothing,
    observation_weights=nothing,
    observation_validity=nothing,
    observation_gap_reasons=nothing,
    structural_ambiguity_reasons=(),
    rank_absolute_low::Real=0.0,
    rank_absolute_high::Real=1e-12,
    rank_relative_low::Real=1e-10,
    rank_relative_high::Real=1e-8,
    backward_error_multiplier::Real=32.0,
    covariance_symmetry_tolerance::Real=1e-12,
    covariance_psd_tolerance::Real=1e-12,
    covariance_positive_definite_floor::Real=1e-12,
    cancel_check=()->nothing,
    limits::ROUncertaintyLimits=ROUncertaintyLimits(),
)
    _rou_cancel(cancel_check)
    limits = _rou_rebuild_limits(limits)
    observation_count, parameter_count = size(sensitivity_values)
    _rou_preflight_identity_source_dimensions(
        identity, source, observation_count, parameter_count, limits)
    identity = _rou_rebuild_field_identity(identity; limits, cancel_check)
    source = _rou_rebuild_scientific_source(source; limits, cancel_check)
    _rou_validate_identity_dimensions(
        identity, source, observation_count, parameter_count, limits)
    (observation_covariance === nothing) ==
        (observation_weights === nothing) && throw(ArgumentError(
            "provide exactly one of observation_covariance or observation_weights"))

    validity = _rou_validity(
        observation_validity,
        observation_count,
        "observation_validity";
        cancel_check,
    )
    valid_count = count(validity)

    element_work = BigInt(observation_count) * parameter_count +
        3 * BigInt(parameter_count)^2
    observation_covariance === nothing ||
        (element_work += BigInt(observation_count)^2)
    _rou_limit(:matrix_elements, element_work, limits.max_matrix_elements)
    factorization_work = BigInt(valid_count) * parameter_count *
        min(valid_count, parameter_count) + BigInt(parameter_count)^3
    observation_covariance === nothing ||
        (factorization_work += BigInt(observation_count)^3 +
            BigInt(valid_count)^3)
    _rou_limit(:factorization_work,
        factorization_work, limits.max_factorization_work)

    sensitivity = _rou_float_matrix(
        sensitivity_values, "sensitivity"; cancel_check)
    gap_reasons = _rou_gap_reasons(
        validity,
        observation_gap_reasons,
        "observation_gap_reasons";
        cancel_check,
    )
    for row in findall(validity)
        all(isfinite, @view sensitivity[row, :]) || throw(ArgumentError(
            "a valid sensitivity row contains non-finite data"))
        _rou_cancel(cancel_check)
    end
    reasons = _rou_structural_reasons(
        structural_ambiguity_reasons; limits, cancel_check)

    absolute_low = _rou_canonical_float(rank_absolute_low)
    absolute_high = _rou_canonical_float(rank_absolute_high)
    relative_low = _rou_canonical_float(rank_relative_low)
    relative_high = _rou_canonical_float(rank_relative_high)
    all(isfinite, (absolute_low, absolute_high, relative_low, relative_high)) &&
        0 <= absolute_low < absolute_high &&
        0 <= relative_low < relative_high || throw(ArgumentError(
            "rank thresholds must be finite and satisfy 0 <= low < high"))
    backward_multiplier = _rou_canonical_float(backward_error_multiplier)
    isfinite(backward_multiplier) && backward_multiplier >= 1 ||
        throw(ArgumentError(
            "backward_error_multiplier must be finite and at least one"))
    symmetry_tolerance = _rou_relative_tolerance(
        covariance_symmetry_tolerance, "covariance_symmetry_tolerance")
    symmetry_tolerance <= RO_COVARIANCE_MAX_SYMMETRY_RELATIVE_TOLERANCE ||
        throw(ArgumentError(
            "covariance_symmetry_tolerance exceeds the hard safety ceiling"))
    psd_tolerance = _rou_relative_tolerance(
        covariance_psd_tolerance, "covariance_psd_tolerance")
    pd_floor = _rou_relative_tolerance(
        covariance_positive_definite_floor,
        "covariance_positive_definite_floor",
    )
    pd_floor > 0 || throw(ArgumentError(
        "covariance_positive_definite_floor must be positive"))

    valid_indices = findall(validity)
    valid_sensitivity = sensitivity[valid_indices, :]
    observation_model = nothing
    experimental_status = :not_supplied
    whitened = Matrix{Float64}(undef, 0, parameter_count)
    _rou_cancel(cancel_check)
    if observation_weights !== nothing
        observation_weights isa AbstractVector || throw(ArgumentError(
            "observation_weights must be a Float64 vector"))
        length(observation_weights) == observation_count ||
            throw(DimensionMismatch(
                "observation_weights must match sensitivity rows"))
        weights = _rou_float_vector(
            observation_weights, "observation_weights"; cancel_check)
        all(value -> isfinite(value) && value > 0, weights) ||
            throw(ArgumentError(
                "observation_weights must be finite and strictly positive"))
        valid_weights = weights[valid_indices]
        whitened = valid_sensitivity .* sqrt.(valid_weights)
        observation_model = (
            kind=:diagonal_precision_weights,
            values=Tuple(weights),
            valid_indices=Tuple(valid_indices),
        )
        experimental_status = :observation_precision_weights_supplied
    else
        observation_covariance isa AbstractMatrix ||
            throw(ArgumentError(
                "observation_covariance must be a Float64 matrix"))
        size(observation_covariance) == (observation_count, observation_count) ||
            throw(DimensionMismatch(
                "observation_covariance must be square over sensitivity rows"))
        covariance_raw = _rou_float_matrix(
            observation_covariance, "observation_covariance"; cancel_check)
        covariance, symmetry_residual, symmetry_admission, covariance_scale =
            _rou_symmetric_matrix(
                covariance_raw,
                "observation_covariance",
                symmetry_tolerance,
            )
        full_factor_work = BigInt(observation_count)^3
        _rou_limit(:factorization_work,
            full_factor_work, limits.max_factorization_work)
        _rou_psd_eigen(
            covariance,
            "observation_covariance",
            psd_tolerance;
            cancel_check,
        )
        valid_covariance = covariance[valid_indices, valid_indices]
        pd_admission = nothing
        if !isempty(valid_indices)
            valid_decomposition, valid_scale, valid_backward_floor, _ =
                _rou_psd_eigen(
                    valid_covariance,
                    "valid observation_covariance submatrix",
                    psd_tolerance;
                    cancel_check,
                )
            minimum_value = minimum(valid_decomposition.values)
            pd_admission = max(pd_floor * valid_scale, valid_backward_floor)
            minimum_value > pd_admission || throw(ArgumentError(
                "observation_covariance is not safely positive definite; " *
                "a finite Fisher precision cannot be formed"))
            _rou_cancel(cancel_check)
            factor = cholesky(Symmetric(valid_covariance))
            whitened = factor.L \ valid_sensitivity
            _rou_cancel(cancel_check)
        end
        observation_model = (
            kind=:observation_covariance,
            supplied_values=_rou_matrix_payload(
                covariance_raw; cancel_check),
            values=_rou_matrix_payload(covariance; cancel_check),
            valid_indices=Tuple(valid_indices),
            valid_submatrix=_rou_matrix_payload(
                valid_covariance; cancel_check),
            admission_policy=(
                symmetry_relative_tolerance=symmetry_tolerance,
                symmetry_residual=symmetry_residual,
                symmetry_admission=symmetry_admission,
                covariance_scale=covariance_scale,
                psd_relative_tolerance=psd_tolerance,
                positive_definite_relative_floor=pd_floor,
                positive_definite_effective_floor=pd_admission,
            ),
        )
        experimental_status = :observation_covariance_supplied
    end

    partition = _rou_partition(
        validity,
        gap_reasons,
        reasons;
        parametric=:not_supplied,
        experimental=experimental_status,
    )
    common_status = _rou_common_status(validity, reasons)

    if isempty(valid_indices)
        requested_low, requested_high, effective_low, effective_high,
            backward_floor, floor_dominated = _rou_rank_thresholds(
                0.0,
                0,
                parameter_count,
                absolute_low,
                absolute_high,
                relative_low,
                relative_high,
                backward_multiplier,
            )
        payload = _rou_local_identity_payload(
            identity, source, sensitivity, validity, gap_reasons, reasons,
            observation_model, absolute_low, absolute_high, relative_low,
            relative_high, requested_low, requested_high, effective_low,
            effective_high, backward_floor, backward_multiplier,
            floor_dominated;
            cancel_check,
        )
        payload = merge(payload, (result_snapshot=_rou_local_result_snapshot(
            status=common_status,
            fim=nothing,
            fim_singular_values=Float64[],
            whitened_singular_values=Float64[],
            rank_lower_bound=0,
            rank_upper_bound=parameter_count,
            numerical_rank=nothing,
            rank_status=:no_valid_observations,
            condition_number=nothing,
            condition_status=:unavailable_no_valid_observations,
            structural_status=:unknown_numerical_gap,
            practical_status=:unknown_numerical_gap,
            practical_covariance=nothing,
            practical_standard_errors=nothing,
            cancel_check=cancel_check,
        ),))
        return ROLocalIdentifiabilityAnalysis(
            _ROU_CONSTRUCTION_TOKEN,
            RO_LOCAL_IDENTIFIABILITY_VERSION,
            identity,
            source,
            _rou_sha256(payload),
            payload,
            partition,
            common_status,
            nothing,
            Float64[],
            Float64[],
            0,
            parameter_count,
            nothing,
            requested_low,
            requested_high,
            effective_low,
            effective_high,
            backward_floor,
            backward_multiplier,
            floor_dominated,
            :whitened_sensitivity_svd,
            :no_valid_observations,
            nothing,
            :unavailable_no_valid_observations,
            :unknown_numerical_gap,
            :unknown_numerical_gap,
            nothing,
            nothing,
            RO_LOCAL_IDENTIFIABILITY_SCOPE,
            false,
            false,
            false,
        )
    end

    all(isfinite, whitened) || throw(ArgumentError(
        "whitened sensitivity contains non-finite data"))
    _rou_cancel(cancel_check)
    decomposition = svd(whitened; full=false)
    _rou_cancel(cancel_check)
    singular_values = Vector{Float64}(decomposition.S)
    if length(singular_values) < parameter_count
        append!(singular_values,
            zeros(Float64, parameter_count - length(singular_values)))
    end
    singular_scale = first(singular_values)
    requested_low, requested_high, effective_low, effective_high,
        backward_floor, floor_dominated = _rou_rank_thresholds(
            singular_scale,
            length(valid_indices),
            parameter_count,
            absolute_low,
            absolute_high,
            relative_low,
            relative_high,
            backward_multiplier,
        )
    rank_lower = count(>(effective_high), singular_values)
    rank_upper = count(>(effective_low), singular_values)
    rank_status = rank_lower == parameter_count ? :full_rank :
        rank_upper < parameter_count ? :rank_deficient : :ambiguous_threshold
    numerical_rank = rank_lower == rank_upper ? rank_lower : nothing

    fim = Matrix(Symmetric(transpose(whitened) * whitened))
    all(isfinite, fim) || throw(ArgumentError(
        "Fisher information contains non-finite data"))
    fim_spectrum = singular_values .^ 2
    condition_number = nothing
    condition_status = rank_status == :rank_deficient ?
        :unavailable_rank_deficient : :unavailable_ambiguous_threshold
    if rank_status == :full_rank
        ratio = first(singular_values) / last(singular_values)
        condition_number = ratio * ratio
        condition_status = isfinite(condition_number) ? :finite : :overflow
    end

    structural_status = if common_status == :unknown_multiple_evidence_gaps
        :unknown_multiple_evidence_gaps
    elseif common_status == :unknown_numerical_gap
        :unknown_numerical_gap
    elseif common_status == :unknown_structural_ambiguity
        :unknown_structural_ambiguity
    elseif rank_status == :full_rank
        :full_rank_local_sensitivity_under_declared_model
    elseif rank_status == :rank_deficient
        :rank_deficient_local_sensitivity_under_declared_model
    else
        :ambiguous_numerical_rank
    end

    practical_status = common_status == :unknown_numerical_gap ?
        :unknown_numerical_gap :
        common_status == :unknown_structural_ambiguity ?
            :unknown_structural_ambiguity :
        common_status == :unknown_multiple_evidence_gaps ?
            :unknown_multiple_evidence_gaps :
        rank_status == :full_rank ? :local_fim_linearized_precision_available :
        rank_status == :rank_deficient ? :unavailable_rank_deficient :
        :unavailable_ambiguous_rank
    practical_covariance = nothing
    practical_standard_errors = nothing
    if practical_status == :local_fim_linearized_precision_available
        _rou_cancel(cancel_check)
        vectors = Matrix(transpose(decomposition.Vt))
        practical_covariance = Matrix(Symmetric(
            vectors * Diagonal(inv.(singular_values .^ 2)) *
            transpose(vectors),
        ))
        diagonal = diag(practical_covariance)
        all(>=(0.0), diagonal) || throw(ArgumentError(
            "local parameter covariance has a negative variance; " *
            "no variance clipping is performed"))
        practical_standard_errors = sqrt.(diagonal)
        all(isfinite, practical_covariance) &&
            all(isfinite, practical_standard_errors) || throw(ArgumentError(
                "local linearized precision contains non-finite data"))
        _rou_cancel(cancel_check)
    end

    payload = _rou_local_identity_payload(
        identity, source, sensitivity, validity, gap_reasons, reasons,
        observation_model, absolute_low, absolute_high, relative_low,
        relative_high, requested_low, requested_high, effective_low,
        effective_high, backward_floor, backward_multiplier, floor_dominated;
        cancel_check,
    )
    payload = merge(payload, (result_snapshot=_rou_local_result_snapshot(
        status=common_status,
        fim=fim,
        fim_singular_values=fim_spectrum,
        whitened_singular_values=singular_values,
        rank_lower_bound=rank_lower,
        rank_upper_bound=rank_upper,
        numerical_rank=numerical_rank,
        rank_status=rank_status,
        condition_number=condition_number,
        condition_status=condition_status,
        structural_status=structural_status,
        practical_status=practical_status,
        practical_covariance=practical_covariance,
        practical_standard_errors=practical_standard_errors,
        cancel_check=cancel_check,
    ),))
    return ROLocalIdentifiabilityAnalysis(
        _ROU_CONSTRUCTION_TOKEN,
        RO_LOCAL_IDENTIFIABILITY_VERSION,
        identity,
        source,
        _rou_sha256(payload),
        payload,
        partition,
        common_status,
        copy(fim),
        copy(fim_spectrum),
        copy(singular_values),
        rank_lower,
        rank_upper,
        numerical_rank,
        requested_low,
        requested_high,
        effective_low,
        effective_high,
        backward_floor,
        backward_multiplier,
        floor_dominated,
        :whitened_sensitivity_svd,
        rank_status,
        condition_number,
        condition_status,
        structural_status,
        practical_status,
        practical_covariance === nothing ? nothing : copy(practical_covariance),
        practical_standard_errors === nothing ? nothing :
            copy(practical_standard_errors),
        RO_LOCAL_IDENTIFIABILITY_SCOPE,
        false,
        false,
        false,
    )
end

function _rou_delta_identity_payload(
    identity,
    source,
    jacobian,
    validity,
    gap_reasons,
    covariance,
    supplied_covariance,
    reasons,
    covariance_admission,
    output_psd_status,
    output_minimum,
    output_backward_floor,
    ;
    cancel_check=()->nothing,
)
    return (
        schema_version=RO_DELTA_METHOD_UNCERTAINTY_VERSION,
        coordinate_identity=ro_field_evidence_identity_payload(identity),
        scientific_source=ro_scientific_source_identity_payload(source),
        scientific_source_sha256=ro_scientific_source_identity_sha256(source),
        output_validity=Tuple(validity),
        output_gap_reasons=gap_reasons,
        jacobian_rows=_rou_valid_rows_payload(
            jacobian, validity; cancel_check),
        parameter_covariance=_rou_matrix_payload(
            covariance; cancel_check),
        supplied_parameter_covariance=_rou_matrix_payload(
            supplied_covariance; cancel_check),
        structural_ambiguity_reasons=reasons,
        covariance_admission_policy=covariance_admission,
        propagation=(
            method=:psd_factor_pushforward,
            output_psd_status=output_psd_status,
            output_minimum_eigenvalue=output_minimum,
            output_backward_error_floor=output_backward_floor,
            variance_clipping_performed=false,
        ),
        method_scope=:first_order_local_delta_method_only,
    )
end

function _rou_delta_result_snapshot(;
    status,
    validity,
    parameter_factor,
    output_factor,
    output_covariance,
    output_standard_deviations,
    output_psd_status,
    output_minimum,
    output_backward_floor,
    cancel_check=()->nothing,
)
    return (
        status=status,
        parameter_covariance_factor=_rou_matrix_payload(
            parameter_factor; cancel_check),
        output_covariance_factor=
            _rou_valid_rows_payload(
                output_factor, validity; cancel_check),
        output_covariance_valid_submatrix=
            _rou_valid_submatrix_payload(
                output_covariance, validity; cancel_check),
        output_standard_deviations=
            _rou_valid_vector_payload(
                output_standard_deviations, validity; cancel_check),
        valid_output_count=count(validity),
        invalid_output_count=length(validity) - count(validity),
        output_psd_status=output_psd_status,
        output_covariance_minimum_eigenvalue=output_minimum,
        output_covariance_backward_error_floor=output_backward_floor,
    )
end

"""
    propagate_ro_delta_covariance(jacobian, parameter_covariance;
        identity, source, ...)

Propagate a PSD factor rather than clipping an assembled covariance. Invalid
rows and every incident entry remain NaN. Negative eigenvalues inside the
admission grey zone raise `ROCovarianceNumericallyAmbiguous`; they are never
silently projected to zero.
"""
function propagate_ro_delta_covariance(
    jacobian_values::AbstractMatrix{<:Real},
    parameter_covariance_values::AbstractMatrix{<:Real};
    identity::ROFieldEvidenceIdentity,
    source::ROScientificSourceIdentity,
    output_validity=nothing,
    output_gap_reasons=nothing,
    structural_ambiguity_reasons=(),
    covariance_symmetry_tolerance::Real=1e-12,
    covariance_psd_tolerance::Real=1e-12,
    cancel_check=()->nothing,
    limits::ROUncertaintyLimits=ROUncertaintyLimits(),
)
    _rou_cancel(cancel_check)
    limits = _rou_rebuild_limits(limits)
    output_count, parameter_count = size(jacobian_values)
    _rou_preflight_identity_source_dimensions(
        identity, source, output_count, parameter_count, limits)
    identity = _rou_rebuild_field_identity(identity; limits, cancel_check)
    source = _rou_rebuild_scientific_source(source; limits, cancel_check)
    _rou_validate_identity_dimensions(
        identity, source, output_count, parameter_count, limits)
    size(parameter_covariance_values) == (parameter_count, parameter_count) ||
        throw(DimensionMismatch(
            "parameter_covariance must be square over parameter_order"))
    validity = _rou_validity(
        output_validity, output_count, "output_validity"; cancel_check)
    valid_count = count(validity)
    element_work = BigInt(output_count) * parameter_count +
        2 * BigInt(parameter_count)^2 + 2 * BigInt(output_count)^2 +
        BigInt(output_count) * parameter_count
    _rou_limit(:matrix_elements, element_work, limits.max_matrix_elements)
    factorization_work = BigInt(parameter_count)^3 + BigInt(valid_count)^3 +
        BigInt(valid_count) * BigInt(parameter_count)^2 +
        BigInt(valid_count)^2 * parameter_count
    _rou_limit(:factorization_work,
        factorization_work, limits.max_factorization_work)

    jacobian = _rou_float_matrix(
        jacobian_values, "Jacobian"; cancel_check)
    gap_reasons = _rou_gap_reasons(
        validity, output_gap_reasons, "output_gap_reasons"; cancel_check)
    for row in findall(validity)
        all(isfinite, @view jacobian[row, :]) || throw(ArgumentError(
            "a valid Jacobian row contains non-finite data"))
        _rou_cancel(cancel_check)
    end
    reasons = _rou_structural_reasons(
        structural_ambiguity_reasons; limits, cancel_check)
    symmetry_tolerance = _rou_relative_tolerance(
        covariance_symmetry_tolerance, "covariance_symmetry_tolerance")
    symmetry_tolerance <= RO_COVARIANCE_MAX_SYMMETRY_RELATIVE_TOLERANCE ||
        throw(ArgumentError(
            "covariance_symmetry_tolerance exceeds the hard safety ceiling"))
    psd_tolerance = _rou_relative_tolerance(
        covariance_psd_tolerance, "covariance_psd_tolerance")
    parameter_covariance_raw = _rou_float_matrix(
        parameter_covariance_values, "parameter_covariance"; cancel_check)
    parameter_covariance, symmetry_residual, symmetry_admission, covariance_scale =
        _rou_symmetric_matrix(
            parameter_covariance_raw,
            "parameter_covariance",
            symmetry_tolerance,
        )
    parameter_decomposition, eigen_scale, backward_floor, ambiguity_floor =
        _rou_psd_eigen(
            parameter_covariance,
            "parameter_covariance",
            psd_tolerance;
            cancel_check,
        )
    parameter_factor = parameter_decomposition.vectors *
        Diagonal(sqrt.(parameter_decomposition.values))
    all(isfinite, parameter_factor) || throw(ArgumentError(
        "parameter covariance factor contains non-finite data"))

    output_covariance = fill(NaN, output_count, output_count)
    output_factor = fill(NaN, output_count, parameter_count)
    output_standard_deviations = fill(NaN, output_count)
    valid_indices = findall(validity)
    output_psd_status = :unavailable_no_valid_outputs
    output_minimum = nothing
    output_backward_floor = nothing
    if !isempty(valid_indices)
        _rou_cancel(cancel_check)
        valid_factor = jacobian[valid_indices, :] * parameter_factor
        valid_covariance = valid_factor * transpose(valid_factor)
        all(isfinite, valid_covariance) || throw(ArgumentError(
            "delta-method output covariance contains non-finite data"))
        valid_covariance = Matrix(Symmetric(valid_covariance))
        _rou_cancel(cancel_check)
        output_spectrum = eigvals(Symmetric(valid_covariance))
        _rou_cancel(cancel_check)
        all(isfinite, output_spectrum) || throw(ArgumentError(
            "delta-method output covariance spectrum is non-finite"))
        output_scale = max(
            _rou_matrix_scale(valid_covariance),
            maximum(abs, output_spectrum; init=0.0),
        )
        output_backward_floor = 32 * eps(Float64) *
            max(1, length(valid_indices)) * output_scale
        output_minimum = minimum(output_spectrum)
        output_minimum < -output_backward_floor && throw(ArgumentError(
            "factor-propagated output covariance violates its PSD backward bound"))
        output_psd_status = output_minimum < 0 ?
            :factor_propagated_roundoff_grey_zone : :factor_propagated_psd
        output_factor[valid_indices, :] .= valid_factor
        output_covariance[valid_indices, valid_indices] .= valid_covariance
        for (local_index, output_index) in enumerate(valid_indices)
            _rou_cancel(cancel_check)
            output_standard_deviations[output_index] =
                norm(@view valid_factor[local_index, :])
        end
    end

    partition = _rou_partition(
        validity,
        gap_reasons,
        reasons;
        parametric=:parameter_covariance_supplied,
        experimental=:not_supplied,
    )
    status = _rou_common_status(validity, reasons)
    covariance_admission = (
        symmetry_relative_tolerance=symmetry_tolerance,
        symmetry_residual=symmetry_residual,
        symmetry_admission=symmetry_admission,
        covariance_scale=covariance_scale,
        psd_relative_tolerance=psd_tolerance,
        eigen_scale=eigen_scale,
        backward_error_floor=backward_floor,
        ambiguity_floor=ambiguity_floor,
    )
    payload = _rou_delta_identity_payload(
        identity,
        source,
        jacobian,
        validity,
        gap_reasons,
        parameter_covariance,
        parameter_covariance_raw,
        reasons,
        covariance_admission,
        output_psd_status,
        output_minimum,
        output_backward_floor;
        cancel_check,
    )
    payload = merge(payload, (result_snapshot=_rou_delta_result_snapshot(
        status=status,
        validity=validity,
        parameter_factor=parameter_factor,
        output_factor=output_factor,
        output_covariance=output_covariance,
        output_standard_deviations=output_standard_deviations,
        output_psd_status=output_psd_status,
        output_minimum=output_minimum,
        output_backward_floor=output_backward_floor,
        cancel_check=cancel_check,
    ),))
    return RODeltaMethodCovariance(
        _ROU_CONSTRUCTION_TOKEN,
        RO_DELTA_METHOD_UNCERTAINTY_VERSION,
        identity,
        source,
        _rou_sha256(payload),
        payload,
        partition,
        status,
        copy(parameter_factor),
        copy(output_factor),
        copy(output_covariance),
        copy(output_standard_deviations),
        length(valid_indices),
        output_count - length(valid_indices),
        psd_tolerance,
        output_psd_status,
        output_minimum,
        output_backward_floor,
        :psd_factor_pushforward,
        :first_order_local_delta_method_only,
        :not_assessed,
        false,
        false,
    )
end

function _rou_validated_ids(
    values,
    expected_count::Int,
    label::AbstractString;
    cancel_check=()->nothing,
)
    ids = _rou_string_tuple(
        values, label; unique_names=true, cancel_check)
    length(ids) == expected_count || throw(DimensionMismatch(
        "$label must contain the complete expected population"))
    return ids
end

function _rou_coverage_result_snapshot(;
    valid_case_count,
    invalid_case_count,
    invalid_case_ids,
    invalid_gap_reasons,
    feature_coverage_counts,
    feature_valid_counts,
    feature_coverage,
    joint_coverage_count,
    joint_valid_count,
    joint_coverage,
    status,
    calibration_status,
    cancel_check=()->nothing,
)
    _rou_cancel(cancel_check)
    return (
        valid_case_count=valid_case_count,
        invalid_case_count=invalid_case_count,
        invalid_case_ids=Tuple(invalid_case_ids),
        invalid_gap_reasons=Tuple(invalid_gap_reasons),
        feature_coverage_counts=Tuple(feature_coverage_counts),
        feature_valid_counts=Tuple(feature_valid_counts),
        feature_coverage=Tuple(feature_coverage),
        joint_coverage_count=joint_coverage_count,
        joint_valid_count=joint_valid_count,
        joint_coverage=joint_coverage,
        status=status,
        calibration_status=calibration_status,
        experimentally_calibrated=false,
    )
end

function evaluate_ro_synthetic_coverage_fixture(
    truth_values::AbstractMatrix{<:Real},
    lower_values::AbstractMatrix{<:Real},
    upper_values::AbstractMatrix{<:Real};
    fixture_id::AbstractString,
    source_fixture_sha256::AbstractString,
    feature_ids,
    case_ids,
    expected_case_count::Integer=length(case_ids),
    case_validity=nothing,
    case_gap_reasons=nothing,
    cancel_check=()->nothing,
    limits::ROUncertaintyLimits=ROUncertaintyLimits(),
)
    _rou_cancel(cancel_check)
    limits = _rou_rebuild_limits(limits)
    expected_case_count isa Bool && throw(ArgumentError(
        "expected_case_count must be an integer, not Bool"))
    expected_case_count > 0 || throw(ArgumentError(
        "expected_case_count must be positive"))
    expected_case_count <= typemax(Int) || throw(ArgumentError(
        "expected_case_count must fit in Int"))
    case_count, feature_count = size(truth_values)
    feature_count > 0 || throw(ArgumentError(
        "synthetic coverage requires at least one feature"))
    size(lower_values) == (case_count, feature_count) ||
        throw(DimensionMismatch("coverage lower bounds must match truth"))
    size(upper_values) == (case_count, feature_count) ||
        throw(DimensionMismatch("coverage upper bounds must match truth"))
    case_count == expected_case_count || throw(DimensionMismatch(
        "synthetic coverage fixture is missing declared cases"))
    case_count <= limits.max_calibration_cases ||
        throw(ROUncertaintyLimitExceeded(
            :calibration_cases, BigInt(case_count), limits.max_calibration_cases))
    feature_count <= limits.max_outputs || throw(ROUncertaintyLimitExceeded(
        :calibration_features, BigInt(feature_count), limits.max_outputs))
    _rou_limit(:matrix_elements,
        3 * BigInt(case_count) * feature_count,
        limits.max_matrix_elements,
    )
    _rou_preflight_sized_collection(
        feature_ids, "feature_ids", :calibration_features,
        limits.max_outputs) == feature_count || throw(DimensionMismatch(
            "feature_ids must match the synthetic coverage columns"))
    _rou_preflight_sized_collection(
        case_ids, "case_ids", :calibration_cases,
        limits.max_calibration_cases) == case_count || throw(DimensionMismatch(
            "case_ids must contain the complete synthetic fixture"))
    validity = _rou_validity(
        case_validity, case_count, "case_validity"; cancel_check)
    metadata_bytes = Ref(BigInt(0))
    fixture = _rou_bounded_string(
        fixture_id, "fixture_id", metadata_bytes, limits,
        :coverage_metadata_bytes; cancel_check)
    source_hash_text = _rou_bounded_string(
        source_fixture_sha256, "source_fixture_sha256", metadata_bytes,
        limits, :coverage_metadata_bytes; cancel_check)
    fixture_sha = _rou_sha256_string(
        source_hash_text, "source_fixture_sha256")
    features = _rou_bounded_string_tuple(
        feature_ids, feature_count, "feature_ids", metadata_bytes, limits,
        :coverage_metadata_bytes; unique_names=true, cancel_check)
    ids = _rou_bounded_string_tuple(
        case_ids, case_count, "case_ids", metadata_bytes, limits,
        :coverage_metadata_bytes; unique_names=true, cancel_check)
    gap_reasons = _rou_gap_reasons(
        validity, case_gap_reasons, "case_gap_reasons"; cancel_check)
    truth = _rou_float_matrix(
        truth_values, "coverage truth"; cancel_check)
    lower = _rou_float_matrix(
        lower_values, "coverage lower bounds"; cancel_check)
    upper = _rou_float_matrix(
        upper_values, "coverage upper bounds"; cancel_check)
    for row in findall(validity)
        all(isfinite, @view truth[row, :]) &&
            all(isfinite, @view lower[row, :]) &&
            all(isfinite, @view upper[row, :]) || throw(ArgumentError(
                "a valid coverage case must be finite"))
        all(@view(lower[row, :]) .<= @view(upper[row, :])) ||
            throw(ArgumentError(
                "coverage lower bounds must not exceed upper bounds"))
        _rou_cancel(cancel_check)
    end
    _rou_cancel(cancel_check)
    valid_indices = findall(validity)
    feature_counts = zeros(Int, feature_count)
    feature_valid_counts = fill(length(valid_indices), feature_count)
    joint_count = 0
    for (position, row) in enumerate(valid_indices)
        covered_jointly = true
        for feature in 1:feature_count
            _rou_cancel(cancel_check)
            covered = lower[row, feature] <= truth[row, feature] <=
                upper[row, feature]
            feature_counts[feature] += covered
            covered_jointly &= covered
        end
        joint_count += covered_jointly
        position % 256 == 0 && _rou_cancel(cancel_check)
    end
    feature_coverage = Union{Nothing,Float64}[
        isempty(valid_indices) ? nothing :
            feature_counts[index] / length(valid_indices)
        for index in 1:feature_count
    ]
    joint_coverage = isempty(valid_indices) ? nothing :
        joint_count / length(valid_indices)
    invalid_indices = findall(.!validity)
    payload = (
        schema_version=RO_SYNTHETIC_COVERAGE_VERSION,
        fixture_id=fixture,
        source_fixture_sha256=fixture_sha,
        feature_ids=features,
        expected_case_count=case_count,
        case_ids=ids,
        case_validity=Tuple(validity),
        case_gap_reasons=gap_reasons,
        truth_rows=_rou_valid_rows_payload(truth, validity; cancel_check),
        lower_rows=_rou_valid_rows_payload(lower, validity; cancel_check),
        upper_rows=_rou_valid_rows_payload(upper, validity; cancel_check),
        coverage_semantics=:truth_inside_closed_feature_interval,
        calibration_scope=:synthetic_fixture_only,
    )
    complete = all(validity)
    coverage_status = complete ? :complete_synthetic_fixture :
        :unknown_incomplete_synthetic_fixture
    calibration_status = complete ?
        :synthetic_coverage_evaluated_not_calibrated :
        :unknown_incomplete_synthetic_coverage_not_calibrated
    invalid_case_ids = Tuple(ids[index] for index in invalid_indices)
    invalid_reasons = Tuple(
        gap_reasons[index]::Symbol for index in invalid_indices)
    payload = merge(payload, (result_snapshot=_rou_coverage_result_snapshot(
        valid_case_count=length(valid_indices),
        invalid_case_count=length(invalid_indices),
        invalid_case_ids=invalid_case_ids,
        invalid_gap_reasons=invalid_reasons,
        feature_coverage_counts=feature_counts,
        feature_valid_counts=feature_valid_counts,
        feature_coverage=feature_coverage,
        joint_coverage_count=joint_count,
        joint_valid_count=length(valid_indices),
        joint_coverage=joint_coverage,
        status=coverage_status,
        calibration_status=calibration_status,
        cancel_check=cancel_check,
    ),))
    return ROSyntheticCoverageEvidence(
        _ROU_CONSTRUCTION_TOKEN,
        RO_SYNTHETIC_COVERAGE_VERSION,
        fixture,
        fixture_sha,
        features,
        _rou_sha256(payload),
        payload,
        case_count,
        length(valid_indices),
        length(invalid_indices),
        invalid_case_ids,
        invalid_reasons,
        copy(feature_counts),
        copy(feature_valid_counts),
        copy(feature_coverage),
        joint_count,
        length(valid_indices),
        joint_coverage,
        coverage_status,
        calibration_status,
        false,
    )
end

mutable struct _ROUPolicyTraversalState
    elements::BigInt
    bytes::BigInt
    active_vectors::IdDict{Any,Nothing}
end

function _rou_policy_charge!(
    state::_ROUPolicyTraversalState,
    limits::ROUncertaintyLimits,
    cancel_check;
    elements::Integer=1,
    bytes::Integer=0,
)
    state.elements += BigInt(elements)
    state.bytes += BigInt(bytes)
    _rou_limit(:policy_elements, state.elements, limits.max_policy_elements)
    _rou_limit(:policy_bytes, state.bytes, limits.max_policy_bytes)
    _rou_cancel(cancel_check)
    return nothing
end

function _rou_canonical_policy_value(
    value,
    state::_ROUPolicyTraversalState,
    depth::Int,
    limits::ROUncertaintyLimits,
    cancel_check,
)
    depth <= limits.max_policy_depth || throw(ROUncertaintyLimitExceeded(
        :policy_depth, BigInt(depth), limits.max_policy_depth))
    _rou_policy_charge!(state, limits, cancel_check)
    if value === nothing
        return (type="nothing",)
    elseif value isa Bool
        return (type="bool", value=value)
    elseif value isa Union{String,SubString{String}}
        byte_count = ncodeunits(value)
        _rou_policy_charge!(
            state, limits, cancel_check; elements=0, bytes=byte_count)
        return (type="string", value=String(value))
    elseif value isa Symbol
        _rou_policy_charge!(state, limits, cancel_check;
            elements=0, bytes=sizeof(value))
        text = String(value)
        return (type="symbol", value=text)
    elseif value isa Integer && !(value isa Bool)
        integer_value = value isa BigInt ? value : BigInt(value)
        byte_count = ndigits(integer_value; base=10) +
            (integer_value < 0 ? 1 : 0)
        _rou_policy_charge!(
            state, limits, cancel_check; elements=0, bytes=byte_count)
        return (type="integer", value=string(value))
    elseif value isa AbstractFloat
        converted = _rou_canonical_float(value)
        isfinite(converted) || throw(ArgumentError(
            "policy numeric values must be finite"))
        _rou_policy_charge!(state, limits, cancel_check; elements=0, bytes=8)
        return (type="float64", value=converted)
    elseif value isa NamedTuple
        state.elements + BigInt(length(value)) <=
            limits.max_policy_elements ||
            throw(ROUncertaintyLimitExceeded(
                :policy_elements,
                state.elements + BigInt(length(value)),
                limits.max_policy_elements))
        return (
            type="named_tuple",
            entries=Tuple(begin
                _rou_policy_charge!(state, limits, cancel_check;
                    elements=0, bytes=sizeof(name))
                name_text = String(name)
                (name=name_text,
                    value=_rou_canonical_policy_value(
                        getfield(value, name), state, depth + 1,
                        limits, cancel_check))
            end for name in keys(value)),
        )
    elseif value isa Tuple
        state.elements + BigInt(length(value)) <= limits.max_policy_elements ||
            throw(ROUncertaintyLimitExceeded(
                :policy_elements,
                state.elements + BigInt(length(value)),
                limits.max_policy_elements,
            ))
        return (
            type="tuple",
            values=Tuple(_rou_canonical_policy_value(
                item, state, depth + 1, limits, cancel_check)
                for item in value),
        )
    elseif value isa AbstractVector
        haskey(state.active_vectors, value) && throw(ArgumentError(
            "policy specification contains a cycle/self-reference"))
        item_count = length(value)
        state.elements + BigInt(item_count) <= limits.max_policy_elements ||
            throw(ROUncertaintyLimitExceeded(
                :policy_elements,
                state.elements + BigInt(item_count),
                limits.max_policy_elements,
            ))
        state.bytes + 8 * BigInt(item_count) <= limits.max_policy_bytes ||
            throw(ROUncertaintyLimitExceeded(
                :policy_bytes,
                state.bytes + 8 * BigInt(item_count),
                limits.max_policy_bytes,
            ))
        state.active_vectors[value] = nothing
        try
            items = Any[]
            sizehint!(items, item_count)
            for item in value
                push!(items, _rou_canonical_policy_value(
                    item, state, depth + 1, limits, cancel_check))
            end
            return (type="vector", values=Tuple(items))
        finally
            delete!(state.active_vectors, value)
        end
    end
    throw(ArgumentError(
        "policy values must be typed scalars, tuples, vectors, or NamedTuples"))
end


function _rou_canonical_policy_value(
    value;
    limits::ROUncertaintyLimits=ROUncertaintyLimits(),
    cancel_check=()->nothing,
)
    _rou_cancel(cancel_check)
    limits = _rou_rebuild_limits(limits)
    state = _ROUPolicyTraversalState(BigInt(0), BigInt(0), IdDict())
    return _rou_canonical_policy_value(
        value, state, 1, limits, cancel_check)
end

function _rou_validate_canonical_policy_value(
    value,
    label::AbstractString,
    state::_ROUPolicyTraversalState,
    depth::Int,
    limits::ROUncertaintyLimits,
    cancel_check,
)
    depth <= limits.max_policy_depth || throw(ROUncertaintyLimitExceeded(
        :policy_depth, BigInt(depth), limits.max_policy_depth))
    _rou_policy_charge!(state, limits, cancel_check)
    value isa NamedTuple || throw(ArgumentError(
        "$label is not a canonical typed policy value"))
    hasproperty(value, :type) && value.type isa String || throw(ArgumentError(
        "$label is missing its canonical type tag"))
    tag = value.type
    _rou_policy_charge!(state, limits, cancel_check;
        elements=0, bytes=ncodeunits(tag))
    if tag == "nothing"
        keys(value) == (:type,) || throw(ArgumentError(
            "$label has noncanonical nothing fields"))
    elseif tag == "bool"
        keys(value) == (:type, :value) && value.value isa Bool ||
            throw(ArgumentError("$label has a noncanonical bool value"))
    elseif tag in ("string", "symbol")
        keys(value) == (:type, :value) && value.value isa String ||
            throw(ArgumentError("$label has a noncanonical $tag value"))
        _rou_policy_charge!(state, limits, cancel_check;
            elements=0, bytes=ncodeunits(value.value))
    elseif tag == "integer"
        keys(value) == (:type, :value) && value.value isa String ||
            throw(ArgumentError("$label has a noncanonical integer value"))
        _rou_policy_charge!(state, limits, cancel_check;
            elements=0, bytes=ncodeunits(value.value))
        parsed = tryparse(BigInt, value.value)
        parsed !== nothing && string(parsed) == value.value ||
            throw(ArgumentError("$label has a noncanonical integer encoding"))
    elseif tag == "float64"
        keys(value) == (:type, :value) && value.value isa Float64 &&
            isfinite(value.value) &&
            (value.value != 0.0 || !signbit(value.value)) || throw(ArgumentError(
                "$label has a noncanonical Float64 value"))
        _rou_policy_charge!(state, limits, cancel_check;
            elements=0, bytes=8)
    elseif tag == "named_tuple"
        keys(value) == (:type, :entries) && value.entries isa Tuple ||
            throw(ArgumentError("$label has noncanonical named-tuple fields"))
        state.elements + BigInt(length(value.entries)) <=
            limits.max_policy_elements || throw(ROUncertaintyLimitExceeded(
                :policy_elements,
                state.elements + BigInt(length(value.entries)),
                limits.max_policy_elements,
            ))
        names = String[]
        for (index, entry) in enumerate(value.entries)
            entry isa NamedTuple && keys(entry) == (:name, :value) &&
                entry.name isa String && !isempty(entry.name) ||
                throw(ArgumentError(
                    "$label entry $index is not canonical"))
            push!(names, entry.name)
            _rou_policy_charge!(state, limits, cancel_check;
                elements=0, bytes=ncodeunits(entry.name))
            _rou_validate_canonical_policy_value(
                entry.value, "$label.$(entry.name)", state, depth + 1,
                limits, cancel_check)
        end
        allunique(names) || throw(ArgumentError(
            "$label has duplicate named-tuple entries"))
    elseif tag in ("tuple", "vector")
        keys(value) == (:type, :values) && value.values isa Tuple ||
            throw(ArgumentError("$label has noncanonical $tag fields"))
        state.elements + BigInt(length(value.values)) <=
            limits.max_policy_elements || throw(ROUncertaintyLimitExceeded(
                :policy_elements,
                state.elements + BigInt(length(value.values)),
                limits.max_policy_elements,
            ))
        for (index, item) in enumerate(value.values)
            _rou_validate_canonical_policy_value(
                item, "$label[$index]", state, depth + 1,
                limits, cancel_check)
        end
    else
        throw(ArgumentError("$label has unknown canonical type tag $tag"))
    end
    return value
end

function _rou_validate_canonical_policy_value(
    value,
    label::AbstractString;
    limits::ROUncertaintyLimits=ROUncertaintyLimits(),
    cancel_check=()->nothing,
)
    _rou_cancel(cancel_check)
    limits = _rou_rebuild_limits(limits)
    state = _ROUPolicyTraversalState(BigInt(0), BigInt(0), IdDict())
    return _rou_validate_canonical_policy_value(
        value, label, state, 1, limits, cancel_check)
end

function _rou_canonical_named_tuple_names(
    value::NamedTuple;
    limits::ROUncertaintyLimits=ROUncertaintyLimits(),
    cancel_check=()->nothing,
)
    _rou_validate_canonical_policy_value(
        value, "policy specification"; limits, cancel_check)
    value.type == "named_tuple" || throw(ArgumentError(
        "policy specification must canonically encode a NamedTuple"))
    return Tuple(entry.name for entry in value.entries)
end

function _rou_validate_standard_policy(
    policy::ROEnsemblePopulationPolicy;
    limits::ROUncertaintyLimits=ROUncertaintyLimits(),
    cancel_check=()->nothing,
)
    _rou_cancel(cancel_check)
    limits = _rou_rebuild_limits(limits)
    typeof(policy) === ROEnsemblePopulationPolicy || throw(ArgumentError(
        "only the standard ensemble population policy is accepted"))
    policy.uncertainty_class in (:parametric, :experimental) ||
        throw(ArgumentError("invalid ensemble uncertainty_class"))
    policy.draw_count > 0 || throw(ArgumentError(
        "ensemble draw_count must be positive"))
    _rou_limit(
        :replicate_population, BigInt(policy.draw_count), limits.max_replicates)
    byte_count = Ref(BigInt(0))
    id = _rou_bounded_string(
        policy.policy_id, "policy_id", byte_count, limits,
        :policy_metadata_bytes; cancel_check)
    isequal(id, policy.policy_id) || throw(ArgumentError(
        "noncanonical ensemble policy_id"))
    revision = _rou_bounded_string(
        policy.policy_revision, "policy_revision", byte_count, limits,
        :policy_metadata_bytes; cancel_check)
    isequal(revision, policy.policy_revision) || throw(ArgumentError(
        "noncanonical ensemble policy_revision"))
    family = _rou_bounded_symbol_string(
        policy.distribution_family, "distribution_family", byte_count,
        limits, :policy_metadata_bytes; cancel_check)
    policy.distribution_family in (:none, :unknown) && throw(ArgumentError(
        "ensemble distribution_family must be concrete"))
    sampling = _rou_bounded_string(
        policy.sampling_revision, "sampling_revision", byte_count, limits,
        :policy_metadata_bytes; cancel_check)
    isequal(sampling, policy.sampling_revision) || throw(ArgumentError(
        "noncanonical ensemble sampling_revision"))
    names = _rou_canonical_named_tuple_names(
        policy.distribution_specification; limits, cancel_check)
    _rou_reject_reserved_policy_names(
        names,
        (
            :uncertainty_class, :policy_id, :policy_revision, :draw_count,
            :population_count, :distribution_family, :sampling_revision,
            :enumeration, :interval_semantics, :coordinate_ids,
            :coordinate_units, :coordinate_lower, :coordinate_upper,
            :interval_definition_sha256,
            :interval_certificate_sha256,
        ),
        "distribution_specification",
    )
    return policy
end

function _rou_validate_standard_policy(
    policy::ROCertifiedIntervalPopulationPolicy,
    ;
    limits::ROUncertaintyLimits=ROUncertaintyLimits(),
    cancel_check=()->nothing,
)
    _rou_cancel(cancel_check)
    limits = _rou_rebuild_limits(limits)
    typeof(policy) === ROCertifiedIntervalPopulationPolicy ||
        throw(ArgumentError(
            "only the standard certified-interval policy is accepted"))
    policy.uncertainty_class in (:parametric, :experimental) ||
        throw(ArgumentError("invalid interval uncertainty_class"))
    policy.population_count > 0 || throw(ArgumentError(
        "interval population_count must be positive"))
    _rou_limit(
        :replicate_population,
        BigInt(policy.population_count),
        limits.max_replicates,
    )
    policy.enumeration == :explicit_complete_coordinate_population ||
        throw(ArgumentError("interval enumeration is invalid"))
    policy.interval_semantics in (:none, :unknown) && throw(ArgumentError(
        "interval_semantics must be concrete"))
    coordinate_limit = policy.uncertainty_class == :parametric ?
        limits.max_parameters : limits.max_observations
    coordinate_count = _rou_preflight_sized_collection(
        policy.coordinate_ids, "coordinate_ids",
        :replicate_coordinate_dimensions, coordinate_limit)
    _rou_preflight_sized_collection(
        policy.coordinate_units, "coordinate_units",
        :replicate_coordinate_dimensions, coordinate_limit) ==
        coordinate_count || throw(DimensionMismatch(
            "interval coordinate metadata dimensions are invalid"))
    _rou_preflight_sized_collection(
        policy.coordinate_lower, "coordinate_lower",
        :replicate_coordinate_dimensions, coordinate_limit) ==
        coordinate_count || throw(DimensionMismatch(
            "interval coordinate metadata dimensions are invalid"))
    _rou_preflight_sized_collection(
        policy.coordinate_upper, "coordinate_upper",
        :replicate_coordinate_dimensions, coordinate_limit) ==
        coordinate_count || throw(DimensionMismatch(
            "interval coordinate metadata dimensions are invalid"))
    byte_count = Ref(BigInt(0))
    _rou_charge_metadata_bytes!(
        byte_count, 16 * BigInt(coordinate_count), limits,
        :policy_metadata_bytes)
    id = _rou_bounded_string(
        policy.policy_id, "policy_id", byte_count, limits,
        :policy_metadata_bytes; cancel_check)
    isequal(id, policy.policy_id) || throw(ArgumentError(
        "noncanonical interval policy_id"))
    revision = _rou_bounded_string(
        policy.policy_revision, "policy_revision", byte_count, limits,
        :policy_metadata_bytes; cancel_check)
    isequal(revision, policy.policy_revision) || throw(ArgumentError(
        "noncanonical interval policy_revision"))
    _rou_bounded_symbol_string(
        policy.interval_semantics, "interval_semantics", byte_count,
        limits, :policy_metadata_bytes; cancel_check)
    certificate_hash = _rou_bounded_string(
        policy.interval_certificate_sha256, "interval_certificate_sha256",
        byte_count, limits, :policy_metadata_bytes; cancel_check)
    definition_hash = _rou_bounded_string(
        policy.interval_definition_sha256, "interval_definition_sha256",
        byte_count, limits, :policy_metadata_bytes; cancel_check)
    ids = _rou_bounded_string_tuple(
        policy.coordinate_ids, coordinate_count, "coordinate_ids", byte_count,
        limits, :policy_metadata_bytes; unique_names=true, cancel_check)
    units = _rou_bounded_string_tuple(
        policy.coordinate_units, coordinate_count, "coordinate_units",
        byte_count, limits, :policy_metadata_bytes; cancel_check)
    lower = _rou_bounded_finite_float_tuple(
        policy.coordinate_lower, coordinate_count, "coordinate_lower";
        cancel_check)
    upper = _rou_bounded_finite_float_tuple(
        policy.coordinate_upper, coordinate_count, "coordinate_upper";
        cancel_check)
    isequal(policy.coordinate_lower, lower) &&
        isequal(policy.coordinate_upper, upper) || throw(ArgumentError(
            "interval coordinate bounds are noncanonical"))
    !isempty(ids) && length(units) == length(ids) &&
        length(lower) == length(ids) && length(upper) == length(ids) ||
        throw(DimensionMismatch(
            "interval coordinate metadata dimensions are invalid"))
    all(lower[index] <= upper[index] for index in eachindex(lower)) ||
        throw(ArgumentError("interval coordinate bounds are invalid"))
    _rou_sha256_string(certificate_hash,
        "interval_certificate_sha256") == certificate_hash ||
        throw(ArgumentError("noncanonical interval certificate hash"))
    names = _rou_canonical_named_tuple_names(
        policy.interval_specification; limits, cancel_check)
    _rou_reject_reserved_policy_names(
        names,
        (
            :uncertainty_class, :policy_id, :policy_revision,
            :population_count, :draw_count, :distribution_family,
            :sampling_revision, :enumeration, :interval_semantics,
            :coordinate_ids, :coordinate_units, :coordinate_lower,
            :coordinate_upper, :lower, :upper,
            :interval_definition_sha256, :interval_certificate_sha256,
        ),
        "interval_specification",
    )
    expected_definition_hash = _rou_sha256(_rou_interval_definition_payload(
        policy.population_count,
        policy.enumeration,
        policy.interval_semantics,
        ids,
        units,
        lower,
        upper,
        policy.interval_specification,
    ))
    _rou_sha256_string(definition_hash, "interval_definition_sha256") ==
        expected_definition_hash ||
        throw(ArgumentError(
            "interval definition hash does not match typed content"))
    return policy
end

function _rou_validate_standard_policy(
    policy::ROPopulationPolicy;
    limits::ROUncertaintyLimits=ROUncertaintyLimits(),
    cancel_check=()->nothing,
)
    _rou_cancel(cancel_check)
    _rou_rebuild_limits(limits)
    throw(ArgumentError(
        "unsupported external ROPopulationPolicy subtype $(typeof(policy))"))
end

function ro_population_count(
    policy::ROEnsemblePopulationPolicy;
    limits::ROUncertaintyLimits=ROUncertaintyLimits(),
    cancel_check=()->nothing,
)
    _rou_validate_standard_policy(policy; limits, cancel_check)
    return policy.draw_count
end
function ro_population_count(
    policy::ROCertifiedIntervalPopulationPolicy;
    limits::ROUncertaintyLimits=ROUncertaintyLimits(),
    cancel_check=()->nothing,
)
    _rou_validate_standard_policy(policy; limits, cancel_check)
    return policy.population_count
end
function ro_population_count(
    policy::ROPopulationPolicy;
    limits::ROUncertaintyLimits=ROUncertaintyLimits(),
    cancel_check=()->nothing,
)
    _rou_validate_standard_policy(policy; limits, cancel_check)
    return 0
end

function ro_population_policy_kind(
    policy::ROEnsemblePopulationPolicy;
    limits::ROUncertaintyLimits=ROUncertaintyLimits(),
    cancel_check=()->nothing,
)
    _rou_validate_standard_policy(policy; limits, cancel_check)
    return :ensemble_distribution
end
function ro_population_policy_kind(
    policy::ROCertifiedIntervalPopulationPolicy;
    limits::ROUncertaintyLimits=ROUncertaintyLimits(),
    cancel_check=()->nothing,
)
    _rou_validate_standard_policy(policy; limits, cancel_check)
    return :certified_interval
end
function ro_population_policy_kind(
    policy::ROPopulationPolicy;
    limits::ROUncertaintyLimits=ROUncertaintyLimits(),
    cancel_check=()->nothing,
)
    _rou_validate_standard_policy(policy; limits, cancel_check)
    return :unsupported
end

function ro_population_policy_payload(
    policy::ROEnsemblePopulationPolicy;
    limits::ROUncertaintyLimits=ROUncertaintyLimits(),
    cancel_check=()->nothing,
)
    _rou_validate_standard_policy(policy; limits, cancel_check)
    return (
        type="ensemble_distribution",
        uncertainty_class=String(policy.uncertainty_class),
        policy_id=policy.policy_id,
        policy_revision=policy.policy_revision,
        draw_count=policy.draw_count,
        distribution_family=String(policy.distribution_family),
        distribution_specification=policy.distribution_specification,
        sampling_revision=policy.sampling_revision,
    )
end

function ro_population_policy_payload(
    policy::ROCertifiedIntervalPopulationPolicy,
    ;
    limits::ROUncertaintyLimits=ROUncertaintyLimits(),
    cancel_check=()->nothing,
)
    _rou_validate_standard_policy(policy; limits, cancel_check)
    return (
        type="certified_interval",
        uncertainty_class=String(policy.uncertainty_class),
        policy_id=policy.policy_id,
        policy_revision=policy.policy_revision,
        population_count=policy.population_count,
        enumeration=String(policy.enumeration),
        interval_semantics=String(policy.interval_semantics),
        coordinate_ids=policy.coordinate_ids,
        coordinate_units=policy.coordinate_units,
        coordinate_lower=policy.coordinate_lower,
        coordinate_upper=policy.coordinate_upper,
        interval_definition_sha256=policy.interval_definition_sha256,
        interval_certificate_sha256=policy.interval_certificate_sha256,
        interval_specification=policy.interval_specification,
    )
end

function ro_population_policy_payload(
    policy::ROPopulationPolicy;
    limits::ROUncertaintyLimits=ROUncertaintyLimits(),
    cancel_check=()->nothing,
)
    _rou_validate_standard_policy(policy; limits, cancel_check)
    return (;)
end

function _rou_quantile_count(values)
    values isa Union{Tuple,AbstractVector} || throw(ArgumentError(
        "quantile probabilities must be a sized tuple or vector"))
    return length(values)
end

function _rou_quantile_probabilities(values; cancel_check=()->nothing)
    count = _rou_quantile_count(values)
    result = Vector{Float64}(undef, count)
    for (position, value) in enumerate(values)
        value isa Float64 || throw(ArgumentError(
            "quantile probabilities must use Float64 exactly"))
        normalized = _rou_canonical_float(value)
        isfinite(normalized) && 0 <= normalized <= 1 || throw(ArgumentError(
            "quantile probabilities must lie in [0, 1]"))
        result[position] = normalized
        position % 1_024 == 0 && _rou_cancel(cancel_check)
    end
    (length(result) <= 1 || all(result[index] < result[index + 1]
        for index in 1:(length(result) - 1))) || throw(ArgumentError(
        "quantile probabilities must be strictly increasing"))
    return Tuple(result)
end


@inline _rou_log2_work(count::Int) = count <= 1 ? 1 : ndigits(count - 1; base=2)

function _rou_validate_calibration_evidence(
    evidence::ROSyntheticCoverageEvidence,
    expected_feature_ids::Tuple{Vararg{String}},
)
    validate_ro_synthetic_coverage_evidence(evidence)
    evidence.feature_ids == expected_feature_ids || throw(DimensionMismatch(
        "calibration feature_ids must match the summarized outputs"))
    return nothing
end

function _rou_population_result_snapshot(;
    status,
    uncertainty_class,
    policy_kind,
    expected_population_count,
    valid_replicate_count,
    invalid_replicate_count,
    gap_probability,
    feature_quantiles,
    bounds_status,
    calibration_status,
    cancel_check=()->nothing,
)
    return (
        status=status,
        uncertainty_class=uncertainty_class,
        policy_kind=policy_kind,
        expected_population_count=expected_population_count,
        valid_replicate_count=valid_replicate_count,
        invalid_replicate_count=invalid_replicate_count,
        gap_probability=gap_probability,
        feature_quantiles=_rou_optional_matrix_payload(
            feature_quantiles; cancel_check),
        bounds_status=bounds_status,
        calibration_status=calibration_status,
        evidence_scope=:declared_population_only,
        causal_claimed=false,
        global_robustness_claimed=false,
        experimentally_validated=false,
    )
end

function summarize_ro_uncertainty_population(
    feature_values::AbstractMatrix{<:Real};
    identity::ROFieldEvidenceIdentity,
    source::ROScientificSourceIdentity,
    policy::ROPopulationPolicy,
    population_ids,
    replicate_coordinate_ids,
    replicate_coordinates::AbstractMatrix{<:Real},
    replicate_validity=nothing,
    replicate_gap_reasons=nothing,
    quantile_probabilities=(0.05, 0.5, 0.95),
    certified_bounds=nothing,
    structural_ambiguity_reasons=(),
    calibration_evidence::Union{Nothing,ROSyntheticCoverageEvidence}=nothing,
    cancel_check=()->nothing,
    limits::ROUncertaintyLimits=ROUncertaintyLimits(),
)
    _rou_cancel(cancel_check)
    limits = _rou_rebuild_limits(limits)
    replicate_count, feature_count = size(feature_values)
    replicate_coordinate_count = size(replicate_coordinates, 2)
    _rou_preflight_identity_source_dimensions(
        identity,
        source,
        feature_count,
        length(identity.parameter_order),
        limits,
    )
    identity = _rou_rebuild_field_identity(identity; limits, cancel_check)
    source = _rou_rebuild_scientific_source(source; limits, cancel_check)
    (typeof(policy) === ROEnsemblePopulationPolicy ||
        typeof(policy) === ROCertifiedIntervalPopulationPolicy) ||
        throw(ArgumentError(
            "only standard ensemble or certified-interval policies are accepted"))
    _rou_validate_standard_policy(policy; limits, cancel_check)
    uncertainty_class = policy.uncertainty_class
    policy_kind = ro_population_policy_kind(policy; limits, cancel_check)
    expected_population_count = ro_population_count(
        policy; limits, cancel_check)
    policy_payload = ro_population_policy_payload(
        policy; limits, cancel_check)
    feature_count > 0 || throw(ArgumentError(
        "uncertainty population requires at least one feature"))
    size(replicate_coordinates, 1) == replicate_count ||
        throw(DimensionMismatch(
            "replicate_coordinates must have one row per population member"))
    replicate_coordinate_count > 0 || throw(ArgumentError(
        "replicate_coordinates must contain at least one policy variable"))
    coordinate_limit = uncertainty_class == :parametric ?
        limits.max_parameters : limits.max_observations
    replicate_coordinate_count <= coordinate_limit ||
        throw(ROUncertaintyLimitExceeded(
            :replicate_coordinate_dimensions,
            BigInt(replicate_coordinate_count),
            coordinate_limit,
        ))
    replicate_count == expected_population_count || throw(DimensionMismatch(
        "replicate matrix must contain the complete declared population"))
    replicate_count <= limits.max_replicates ||
        throw(ROUncertaintyLimitExceeded(
            :replicate_population, BigInt(replicate_count), limits.max_replicates))
    _rou_preflight_sized_collection(
        population_ids, "population_ids", :replicate_population,
        limits.max_replicates) == replicate_count || throw(DimensionMismatch(
            "population_ids must contain the complete expected population"))
    _rou_preflight_sized_collection(
        replicate_coordinate_ids, "replicate_coordinate_ids",
        :replicate_coordinate_dimensions, coordinate_limit) ==
        replicate_coordinate_count || throw(DimensionMismatch(
            "replicate_coordinate_ids must match replicate_coordinates columns"))
    _rou_validate_identity_dimensions(
        identity, source, feature_count, length(identity.parameter_order), limits)
    validity = _rou_validity(
        replicate_validity,
        replicate_count,
        "replicate_validity";
        cancel_check,
    )
    valid_count = count(validity)
    quantile_count = _rou_quantile_count(quantile_probabilities)
    _rou_limit(:quantiles, BigInt(quantile_count), limits.max_quantiles)
    quantile_elements = BigInt(quantile_count) * feature_count
    _rou_limit(
        :quantile_matrix_elements,
        quantile_elements,
        limits.max_matrix_elements,
    )
    _rou_limit(:matrix_elements,
        BigInt(replicate_count) *
            (feature_count + replicate_coordinate_count) +
            2 * BigInt(feature_count)^2 + quantile_elements,
        limits.max_matrix_elements,
    )
    log_work = _rou_log2_work(valid_count)
    quantile_work = typeof(policy) === ROEnsemblePopulationPolicy ?
        BigInt(feature_count) * log_work *
            (BigInt(valid_count) + quantile_count) : BigInt(0)
    _rou_limit(:sort_work, quantile_work, limits.max_sort_work)

    metadata_bytes = Ref(BigInt(0))
    ids = _rou_bounded_string_tuple(
        population_ids, replicate_count, "population_ids", metadata_bytes,
        limits, :population_metadata_bytes; unique_names=true, cancel_check)
    coordinate_ids = _rou_bounded_string_tuple(
        replicate_coordinate_ids, replicate_coordinate_count,
        "replicate_coordinate_ids", metadata_bytes, limits,
        :population_metadata_bytes; unique_names=true, cancel_check)
    replicate_points = _rou_float_matrix(
        replicate_coordinates, "replicate_coordinates"; cancel_check)
    all(isfinite, replicate_points) || throw(ArgumentError(
        "replicate_coordinates must be finite for the complete population"))
    gap_reasons = _rou_gap_reasons(
        validity,
        replicate_gap_reasons,
        "replicate_gap_reasons";
        cancel_check,
    )
    features = _rou_float_matrix(
        feature_values, "replicate feature values"; cancel_check)
    for row in findall(validity)
        all(isfinite, @view features[row, :]) || throw(ArgumentError(
            "a valid replicate contains non-finite feature values"))
        _rou_cancel(cancel_check)
    end
    reasons = _rou_structural_reasons(
        structural_ambiguity_reasons; limits, cancel_check)
    _rou_cancel(cancel_check)
    calibration_evidence === nothing || _rou_validate_calibration_evidence(
        calibration_evidence, identity.output_order)
    policy_name = policy.policy_id
    policy_version = policy.policy_revision
    probabilities = _rou_quantile_probabilities(
        quantile_probabilities; cancel_check)
    valid_indices = findall(validity)
    invalid_indices = findall(.!validity)

    feature_quantiles = nothing
    quantile_scope = :not_applicable
    bounds = nothing
    certificate_sha = nothing
    bounds_status = :not_applicable
    if typeof(policy) === ROEnsemblePopulationPolicy
        isempty(probabilities) && throw(ArgumentError(
            "ensemble_distribution requires quantile probabilities"))
        certified_bounds === nothing || throw(ArgumentError(
            "ensemble_distribution does not accept certified_bounds"))
        if !isempty(valid_indices)
            feature_quantiles = Matrix{Float64}(
                undef, length(probabilities), feature_count)
            probability_values = collect(probabilities)
            for feature in 1:feature_count
                _rou_cancel(cancel_check)
                feature_quantiles[:, feature] .= quantile(
                    features[valid_indices, feature], probability_values)
            end
        end
        quantile_scope = :empirical_conditional_on_valid_replicates
    elseif typeof(policy) === ROCertifiedIntervalPopulationPolicy
        coordinate_ids == policy.coordinate_ids || throw(DimensionMismatch(
            "replicate_coordinate_ids must exactly match the typed interval " *
            "coordinate order"))
        coordinate_rows = Tuple(
            Tuple(replicate_points[row, :]) for row in axes(replicate_points, 1))
        allunique(coordinate_rows) || throw(ArgumentError(
            "explicit complete coordinate population must contain unique rows"))
        for row in axes(replicate_points, 1),
            coordinate in axes(replicate_points, 2)
            _rou_cancel(cancel_check)
            policy.coordinate_lower[coordinate] <=
                replicate_points[row, coordinate] <=
                policy.coordinate_upper[coordinate] || throw(ArgumentError(
                    "replicate coordinate lies outside typed interval bounds"))
        end
        isempty(probabilities) || throw(ArgumentError(
            "certified_interval requires empty quantile_probabilities"))
        certified_bounds isa AbstractMatrix{<:Real} || throw(ArgumentError(
            "certified_interval requires numeric certified_bounds"))
        size(certified_bounds) == (feature_count, 2) ||
            throw(DimensionMismatch(
                "certified_bounds must have one [lower, upper] row per feature"))
        bounds = _rou_float_matrix(
            certified_bounds, "certified_bounds"; cancel_check)
        all(isfinite, bounds) || throw(ArgumentError(
            "certified_bounds must be finite"))
        all(bounds[:, 1] .<= bounds[:, 2]) || throw(ArgumentError(
            "certified bound lower values must not exceed upper values"))
        for row in valid_indices, feature in 1:feature_count
            _rou_cancel(cancel_check)
            bounds[feature, 1] <= features[row, feature] <=
                bounds[feature, 2] || throw(ArgumentError(
                    "enumerated valid feature population contradicts " *
                    "certified_bounds"))
        end
        certificate_sha = policy.interval_certificate_sha256
        bounds_status = :declared_certificate_bound_to_policy_not_reproved
    else
        throw(ArgumentError("unsupported population policy"))
    end
    _rou_cancel(cancel_check)

    parametric_status = uncertainty_class == :parametric ?
        (policy_kind == :ensemble_distribution ?
            :declared_distribution_population : :declared_interval_population) :
        :not_supplied
    experimental_status = uncertainty_class == :experimental ?
        (policy_kind == :ensemble_distribution ?
            :declared_distribution_population : :declared_interval_population) :
        :not_supplied
    partition = _rou_partition(
        validity,
        gap_reasons,
        reasons;
        parametric=parametric_status,
        experimental=experimental_status,
    )
    status = _rou_common_status(validity, reasons)
    calibration_status = calibration_evidence === nothing ? :not_assessed :
        calibration_evidence.calibration_status
    payload = (
        schema_version=RO_UNCERTAINTY_POPULATION_VERSION,
        coordinate_identity=ro_field_evidence_identity_payload(identity),
        scientific_source=ro_scientific_source_identity_payload(source),
        scientific_source_sha256=ro_scientific_source_identity_sha256(source),
        uncertainty_class=uncertainty_class,
        policy=policy_payload,
        expected_population_count=replicate_count,
        population_ids=ids,
        replicate_coordinate_ids=coordinate_ids,
        replicate_coordinates=_rou_matrix_payload(
            replicate_points; cancel_check),
        replicate_validity=Tuple(validity),
        replicate_gap_reasons=gap_reasons,
        feature_rows=_rou_valid_rows_payload(
            features, validity; cancel_check),
        quantile_probabilities=probabilities,
        quantile_scope=quantile_scope,
        certified_bounds=bounds === nothing ? nothing :
            _rou_matrix_payload(bounds; cancel_check),
        structural_ambiguity_reasons=reasons,
        calibration_evidence_sha256=calibration_evidence === nothing ?
            nothing : calibration_evidence.identity_sha256,
        evidence_scope=:declared_population_only,
    )
    payload = merge(payload, (result_snapshot=_rou_population_result_snapshot(
        status=status,
        uncertainty_class=uncertainty_class,
        policy_kind=policy_kind,
        expected_population_count=replicate_count,
        valid_replicate_count=length(valid_indices),
        invalid_replicate_count=length(invalid_indices),
        gap_probability=length(invalid_indices) / replicate_count,
        feature_quantiles=feature_quantiles,
        bounds_status=bounds_status,
        calibration_status=calibration_status,
        cancel_check=cancel_check,
    ),))
    return ROUncertaintyPopulationArtifact(
        _ROU_CONSTRUCTION_TOKEN,
        RO_UNCERTAINTY_POPULATION_VERSION,
        identity,
        source,
        _rou_sha256(payload),
        payload,
        partition,
        status,
        uncertainty_class,
        policy_kind,
        policy_name,
        policy_version,
        replicate_count,
        ids,
        coordinate_ids,
        copy(replicate_points),
        length(valid_indices),
        length(invalid_indices),
        length(invalid_indices) / replicate_count,
        probabilities,
        feature_quantiles === nothing ? nothing : copy(feature_quantiles),
        quantile_scope,
        bounds === nothing ? nothing : copy(bounds),
        certificate_sha,
        bounds_status,
        calibration_evidence === nothing ? nothing : deepcopy(calibration_evidence),
        calibration_status,
        :declared_population_only,
        false,
        false,
        false,
    )
end

@noinline function _rou_backing_changed(label::AbstractString)
    throw(ArgumentError("$label backing storage changed after admission"))
end

function _rou_sealed_payload(result, label::AbstractString)
    payload = getfield(result, :identity_payload)
    payload isa NamedTuple || _rou_backing_changed(label)
    getfield(result, :identity_sha256) == _rou_sha256(payload) ||
        _rou_backing_changed(label)
    hasproperty(payload, :result_snapshot) || _rou_backing_changed(label)
    return payload
end

function _rou_assert_unchanged(result::ROLocalIdentifiabilityAnalysis)
    payload = _rou_sealed_payload(result, "ROLocalIdentifiabilityAnalysis")
    snapshot = _rou_local_result_snapshot(
        status=getfield(result, :status),
        fim=getfield(result, :fim),
        fim_singular_values=getfield(result, :fim_singular_values),
        whitened_singular_values=
            getfield(result, :whitened_sensitivity_singular_values),
        rank_lower_bound=getfield(result, :rank_lower_bound),
        rank_upper_bound=getfield(result, :rank_upper_bound),
        numerical_rank=getfield(result, :numerical_rank),
        rank_status=getfield(result, :rank_status),
        condition_number=getfield(result, :condition_number),
        condition_status=getfield(result, :condition_status),
        structural_status=
            getfield(result, :structural_local_identifiability),
        practical_status=getfield(result, :practical_precision_status),
        practical_covariance=
            getfield(result, :practical_parameter_covariance),
        practical_standard_errors=
            getfield(result, :practical_parameter_standard_errors),
    )
    isequal(getfield(payload, :result_snapshot), snapshot) ||
        _rou_backing_changed("ROLocalIdentifiabilityAnalysis")
    return nothing
end

function _rou_delta_seal_validity(payload)
    hasproperty(payload, :output_validity) ||
        _rou_backing_changed("RODeltaMethodCovariance")
    values = getfield(payload, :output_validity)
    values isa Tuple || _rou_backing_changed("RODeltaMethodCovariance")
    validity = falses(length(values))
    for index in eachindex(values)
        values[index] isa Bool ||
            _rou_backing_changed("RODeltaMethodCovariance")
        validity[index] = values[index]
    end
    return validity
end

function _rou_assert_unchanged(result::RODeltaMethodCovariance)
    label = "RODeltaMethodCovariance"
    payload = _rou_sealed_payload(result, label)
    validity = _rou_delta_seal_validity(payload)
    output_factor = getfield(result, :output_covariance_factor)
    output_covariance = getfield(result, :output_covariance)
    output_standard_deviations =
        getfield(result, :output_standard_deviations)
    output_count = length(validity)
    size(output_factor, 1) == output_count || _rou_backing_changed(label)
    size(output_covariance) == (output_count, output_count) ||
        _rou_backing_changed(label)
    length(output_standard_deviations) == output_count ||
        _rou_backing_changed(label)
    for index in eachindex(validity)
        validity[index] && continue
        all(isnan, @view output_factor[index, :]) ||
            _rou_backing_changed(label)
        isnan(output_standard_deviations[index]) ||
            _rou_backing_changed(label)
        all(isnan, @view output_covariance[index, :]) ||
            _rou_backing_changed(label)
        all(isnan, @view output_covariance[:, index]) ||
            _rou_backing_changed(label)
    end
    snapshot = _rou_delta_result_snapshot(
        status=getfield(result, :status),
        validity=validity,
        parameter_factor=getfield(result, :parameter_covariance_factor),
        output_factor=output_factor,
        output_covariance=output_covariance,
        output_standard_deviations=output_standard_deviations,
        output_psd_status=getfield(result, :output_psd_status),
        output_minimum=
            getfield(result, :output_covariance_minimum_eigenvalue),
        output_backward_floor=
            getfield(result, :output_covariance_backward_error_floor),
    )
    isequal(getfield(payload, :result_snapshot), snapshot) ||
        _rou_backing_changed(label)
    return nothing
end

function _rou_assert_unchanged(result::ROSyntheticCoverageEvidence)
    label = "ROSyntheticCoverageEvidence"
    payload = _rou_sealed_payload(result, label)
    snapshot = _rou_coverage_result_snapshot(
        valid_case_count=getfield(result, :valid_case_count),
        invalid_case_count=getfield(result, :invalid_case_count),
        invalid_case_ids=getfield(result, :invalid_case_ids),
        invalid_gap_reasons=getfield(result, :invalid_gap_reasons),
        feature_coverage_counts=
            getfield(result, :feature_coverage_counts),
        feature_valid_counts=getfield(result, :feature_valid_counts),
        feature_coverage=getfield(result, :feature_coverage),
        joint_coverage_count=getfield(result, :joint_coverage_count),
        joint_valid_count=getfield(result, :joint_valid_count),
        joint_coverage=getfield(result, :joint_coverage),
        status=getfield(result, :status),
        calibration_status=getfield(result, :calibration_status),
    )
    isequal(getfield(payload, :result_snapshot), snapshot) ||
        _rou_backing_changed(label)
    return nothing
end

function _rou_assert_unchanged(result::ROUncertaintyPopulationArtifact)
    label = "ROUncertaintyPopulationArtifact"
    payload = _rou_sealed_payload(result, label)
    hasproperty(payload, :replicate_coordinates) ||
        _rou_backing_changed(label)
    hasproperty(payload, :certified_bounds) || _rou_backing_changed(label)
    hasproperty(payload, :calibration_evidence_sha256) ||
        _rou_backing_changed(label)
    replicate_coordinates = getfield(result, :replicate_coordinates)
    isequal(
        getfield(payload, :replicate_coordinates),
        _rou_matrix_payload(replicate_coordinates),
    ) || _rou_backing_changed(label)
    certified_bounds = getfield(result, :certified_bounds)
    sealed_bounds = certified_bounds === nothing ? nothing :
        _rou_matrix_payload(certified_bounds)
    isequal(getfield(payload, :certified_bounds), sealed_bounds) ||
        _rou_backing_changed(label)
    calibration = getfield(result, :calibration_evidence)
    calibration_sha256 = getfield(payload, :calibration_evidence_sha256)
    if calibration === nothing
        calibration_sha256 === nothing || _rou_backing_changed(label)
    else
        _rou_assert_unchanged(calibration)
        getfield(calibration, :identity_sha256) == calibration_sha256 ||
            _rou_backing_changed(label)
    end
    snapshot = _rou_population_result_snapshot(
        status=getfield(result, :status),
        uncertainty_class=getfield(result, :uncertainty_class),
        policy_kind=getfield(result, :policy_kind),
        expected_population_count=
            getfield(result, :expected_population_count),
        valid_replicate_count=getfield(result, :valid_replicate_count),
        invalid_replicate_count=getfield(result, :invalid_replicate_count),
        gap_probability=getfield(result, :gap_probability),
        feature_quantiles=getfield(result, :feature_quantiles),
        bounds_status=getfield(result, :bounds_status),
        calibration_status=getfield(result, :calibration_status),
    )
    isequal(getfield(payload, :result_snapshot), snapshot) ||
        _rou_backing_changed(label)
    return nothing
end

function Base.getproperty(result::ROLocalIdentifiabilityAnalysis, name::Symbol)
    _rou_assert_unchanged(result)
    value = getfield(result, name)
    if name in (
        :fim,
        :fim_singular_values,
        :whitened_sensitivity_singular_values,
        :practical_parameter_covariance,
        :practical_parameter_standard_errors,
    )
        return value === nothing ? nothing : copy(value)
    end
    return value
end

function Base.getproperty(result::RODeltaMethodCovariance, name::Symbol)
    _rou_assert_unchanged(result)
    value = getfield(result, name)
    if name in (
        :parameter_covariance_factor,
        :output_covariance_factor,
        :output_covariance,
        :output_standard_deviations,
    )
        return copy(value)
    end
    return value
end

function Base.getproperty(result::ROSyntheticCoverageEvidence, name::Symbol)
    _rou_assert_unchanged(result)
    value = getfield(result, name)
    if name in (
        :feature_coverage_counts,
        :feature_valid_counts,
        :feature_coverage,
    )
        return copy(value)
    end
    return value
end

function Base.getproperty(result::ROUncertaintyPopulationArtifact, name::Symbol)
    _rou_assert_unchanged(result)
    value = getfield(result, name)
    if name in (:replicate_coordinates, :feature_quantiles, :certified_bounds)
        return value === nothing ? nothing : copy(value)
    elseif name === :calibration_evidence
        return value === nothing ? nothing : deepcopy(value)
    end
    return value
end

@inline function _rou_validate(condition::Bool, message::AbstractString)
    condition || throw(ArgumentError(String(message)))
    return nothing
end

function _rou_require_exact_keys(value, expected::Tuple, label::AbstractString)
    value isa NamedTuple || throw(ArgumentError(
        "$label must be a NamedTuple"))
    keys(value) == expected || throw(ArgumentError(
        "$label fields must be exactly $expected; received $(keys(value))"))
    return value
end

function _rou_validate_canonical_payload_floats(
    value,
    label::AbstractString;
    cancel_check=()->nothing,
    visited::Base.RefValue{Int}=Ref(0),
)
    visited[] += 1
    visited[] % 4_096 == 0 && _rou_cancel(cancel_check)
    if value isa Float64
        isfinite(value) || throw(ArgumentError(
            "$label contains a non-finite Float64"))
        (value != 0.0 || !signbit(value)) || throw(ArgumentError(
            "$label contains noncanonical -0.0"))
    elseif value isa AbstractFloat
        throw(ArgumentError(
            "$label contains a non-Float64 scientific numeric value"))
    elseif value isa NamedTuple
        for name in keys(value)
            _rou_validate_canonical_payload_floats(
                getproperty(value, name), "$label.$name";
                cancel_check, visited)
        end
    elseif value isa Tuple
        for (index, item) in enumerate(value)
            _rou_validate_canonical_payload_floats(
                item, "$label[$index]"; cancel_check, visited)
        end
    end
    return nothing
end

function _rou_validate_coordinate_identity_schema(value)
    _rou_require_exact_keys(
        value,
        (:schema_version, :inputs, :parameters, :outputs),
        "coordinate_identity",
    )
    for section_name in (:inputs, :parameters, :outputs)
        _rou_require_exact_keys(
            getproperty(value, section_name),
            (:order, :units, :scales),
            "coordinate_identity.$section_name",
        )
    end
    return nothing
end

function _rou_validate_scientific_source_schema(value)
    _rou_require_exact_keys(
        value,
        (
            :schema_version, :source_field_sha256, :field_request_sha256,
            :network_sha256, :local_coordinates, :nominal_parameters,
            :observation_schedule, :solver_revision, :algorithm_revision,
        ),
        "scientific_source",
    )
    return nothing
end

function _rou_payload_validity(values, count::Int, label::AbstractString)
    values isa Tuple || throw(ArgumentError("$label must be a Tuple"))
    length(values) == count || throw(DimensionMismatch(
        "$label length must be $count"))
    result = falses(count)
    for index in 1:count
        values[index] isa Bool || throw(ArgumentError(
            "$label[$index] must be Bool"))
        result[index] = values[index]
    end
    return result
end

function _rou_payload_valid_rows(
    rows,
    validity::BitVector,
    column_count::Int,
    label::AbstractString,
)
    rows isa Tuple || throw(ArgumentError("$label must be a Tuple"))
    length(rows) == length(validity) || throw(DimensionMismatch(
        "$label must match validity"))
    result = fill(NaN, length(validity), column_count)
    for row_index in eachindex(validity)
        row = rows[row_index]
        if validity[row_index]
            row isa Tuple && length(row) == column_count ||
                throw(DimensionMismatch(
                    "$label[$row_index] must contain every feature"))
            for column in 1:column_count
                row[column] isa Float64 || throw(ArgumentError(
                    "$label[$row_index][$column] must be Float64"))
                isfinite(row[column]) || throw(ArgumentError(
                    "$label valid rows must be finite"))
                result[row_index, column] = row[column]
            end
        else
            row === nothing || throw(ArgumentError(
                "$label invalid rows must be explicit nothing gaps"))
        end
    end
    return result
end

function _rou_payload_matrix(
    rows,
    row_count::Int,
    column_count::Int,
    label::AbstractString,
)
    validity = trues(row_count)
    return _rou_payload_valid_rows(rows, validity, column_count, label)
end

function _rou_numeric_equal(actual, expected)
    if actual === nothing || expected === nothing
        return actual === expected
    elseif actual isa Number && expected isa Number
        return isequal(actual, expected) || isapprox(
            actual, expected; rtol=512 * eps(Float64), atol=0.0, nans=true)
    elseif actual isa AbstractArray && expected isa AbstractArray
        size(actual) == size(expected) || return false
        return all(_rou_numeric_equal(a, b) for (a, b) in zip(actual, expected))
    end
    return isequal(actual, expected)
end

function _rou_validate_numeric(actual, expected, label::AbstractString)
    _rou_numeric_equal(actual, expected) || throw(ArgumentError(
        "$label does not match independently recomputed evidence"))
    return nothing
end

@inline function _rou_maximum_abs(values)
    return maximum(abs, values; init=0.0)
end

function _rou_matmul_backward_bound(left, right)
    inner = size(left, 2)
    scale = _rou_maximum_abs(left) * _rou_maximum_abs(right) *
        max(1, inner)
    return 128 * eps(Float64) * max(scale, floatmin(Float64))
end

function _rou_validate_absolute_bound(
    actual,
    expected,
    bound::Float64,
    label::AbstractString,
)
    size(actual) == size(expected) || throw(DimensionMismatch(
        "$label dimensions do not match"))
    all(isfinite, actual) && all(isfinite, expected) || throw(ArgumentError(
        "$label must be finite"))
    residual = maximum(abs, actual .- expected; init=0.0)
    residual <= bound || throw(ArgumentError(
        "$label residual $residual exceeds backward-error bound $bound"))
    return nothing
end

function _rou_validate_partition(
    actual::ROUncertaintyPartition,
    expected::ROUncertaintyPartition,
    label::AbstractString,
)
    for field in fieldnames(ROUncertaintyPartition)
        isequal(getfield(actual, field), getfield(expected, field)) ||
            throw(ArgumentError("$label partition field $field is invalid"))
    end
    return nothing
end

function _rou_validate_identity_envelope(
    result,
    expected_schema::String;
    limits::ROUncertaintyLimits=ROUncertaintyLimits(),
    cancel_check=()->nothing,
)
    _rou_cancel(cancel_check)
    limits = _rou_rebuild_limits(limits)
    _rou_validate(result.schema_version == expected_schema,
        "artifact schema version mismatch")
    payload = result.identity_payload
    _rou_validate(payload.schema_version == expected_schema,
        "payload schema version mismatch")
    _rou_validate_coordinate_identity_schema(payload.coordinate_identity)
    _rou_validate_scientific_source_schema(payload.scientific_source)
    _rou_preflight_identity_source_dimensions(
        result.identity,
        result.source,
        length(result.identity.output_order),
        length(result.identity.parameter_order),
        limits,
    )
    identity = _rou_rebuild_field_identity(
        result.identity; limits, cancel_check)
    source = _rou_rebuild_scientific_source(
        result.source; limits, cancel_check)
    _rou_validate(isequal(result.identity, identity),
        "stored coordinate identity is noncanonical")
    _rou_validate(isequal(result.source, source),
        "stored scientific source is noncanonical")
    _rou_validate(
        isequal(payload.coordinate_identity,
            ro_field_evidence_identity_payload(identity)),
        "coordinate identity mismatch",
    )
    _rou_validate(
        isequal(payload.scientific_source,
            ro_scientific_source_identity_payload(source)),
        "scientific source mismatch",
    )
    _rou_validate(payload.scientific_source_sha256 ==
        ro_scientific_source_identity_sha256(source),
        "scientific source SHA-256 mismatch")
    return payload, identity, source
end

function _rou_validate_population_policy_payload(
    policy;
    limits::ROUncertaintyLimits=ROUncertaintyLimits(),
    cancel_check=()->nothing,
)
    _rou_cancel(cancel_check)
    limits = _rou_rebuild_limits(limits)
    policy isa NamedTuple || throw(ArgumentError(
        "population policy payload must be a NamedTuple"))
    hasproperty(policy, :type) || throw(ArgumentError(
        "population policy payload is missing type"))
    policy.type isa String || throw(ArgumentError(
        "population policy type must be a String"))
    byte_count = Ref(BigInt(0))
    policy_type = _rou_bounded_string(
        policy.type, "population policy type", byte_count, limits,
        :policy_metadata_bytes; cancel_check)
    if policy_type == "ensemble_distribution"
        keys(policy) == (
            :type, :uncertainty_class, :policy_id, :policy_revision,
            :draw_count, :distribution_family,
            :distribution_specification, :sampling_revision,
        ) || throw(ArgumentError("ensemble policy payload fields are invalid"))
    elseif policy_type == "certified_interval"
        keys(policy) == (
            :type, :uncertainty_class, :policy_id, :policy_revision,
            :population_count, :enumeration, :interval_semantics,
            :coordinate_ids, :coordinate_units, :coordinate_lower,
            :coordinate_upper,
            :interval_definition_sha256, :interval_certificate_sha256,
            :interval_specification,
        ) || throw(ArgumentError("interval policy payload fields are invalid"))
    else
        throw(ArgumentError("unsupported population policy payload type"))
    end
    policy.uncertainty_class isa String || throw(ArgumentError(
        "population uncertainty_class must be a String"))
    uncertainty_text = _rou_bounded_string(
        policy.uncertainty_class, "population uncertainty_class", byte_count,
        limits, :policy_metadata_bytes; cancel_check)
    uncertainty_class = if uncertainty_text == "parametric"
        :parametric
    elseif uncertainty_text == "experimental"
        :experimental
    else
        throw(ArgumentError("population uncertainty_class is invalid"))
    end
    if policy_type == "ensemble_distribution"
        policy.draw_count isa Int && policy.draw_count > 0 ||
            throw(ArgumentError("ensemble draw_count is invalid"))
        _rou_limit(
            :replicate_population,
            BigInt(policy.draw_count),
            limits.max_replicates,
        )
        policy_id = _rou_bounded_string(
            policy.policy_id, "policy_id", byte_count, limits,
            :policy_metadata_bytes; cancel_check)
        policy_revision = _rou_bounded_string(
            policy.policy_revision, "policy_revision", byte_count, limits,
            :policy_metadata_bytes; cancel_check)
        distribution_family = _rou_bounded_string(
            policy.distribution_family, "distribution_family", byte_count,
            limits, :policy_metadata_bytes; cancel_check)
        distribution_family in ("none", "unknown") && throw(ArgumentError(
            "ensemble distribution_family is invalid"))
        sampling_revision = _rou_bounded_string(
            policy.sampling_revision, "sampling_revision", byte_count, limits,
            :policy_metadata_bytes; cancel_check)
        names = _rou_canonical_named_tuple_names(
            policy.distribution_specification; limits, cancel_check)
        _rou_reject_reserved_policy_names(
            names,
            (
                :uncertainty_class, :policy_id, :policy_revision, :draw_count,
                :population_count, :distribution_family, :sampling_revision,
                :enumeration, :interval_semantics, :coordinate_ids,
                :coordinate_units, :coordinate_lower, :coordinate_upper,
                :interval_definition_sha256,
                :interval_certificate_sha256,
            ),
            "distribution_specification",
        )
        return (
            kind=:ensemble_distribution,
            uncertainty_class,
            policy_id,
            policy_revision,
            population_count=policy.draw_count,
            interval_certificate_sha256=nothing,
        )
    elseif policy_type == "certified_interval"
        policy.population_count isa Int && policy.population_count > 0 ||
            throw(ArgumentError("interval population_count is invalid"))
        _rou_limit(
            :replicate_population,
            BigInt(policy.population_count),
            limits.max_replicates,
        )
        policy.enumeration isa String &&
            policy.enumeration ==
                "explicit_complete_coordinate_population" ||
            throw(ArgumentError("interval enumeration is invalid"))
        enumeration = _rou_bounded_string(
            policy.enumeration, "enumeration", byte_count, limits,
            :policy_metadata_bytes; cancel_check)
        policy_id = _rou_bounded_string(
            policy.policy_id, "policy_id", byte_count, limits,
            :policy_metadata_bytes; cancel_check)
        policy_revision = _rou_bounded_string(
            policy.policy_revision, "policy_revision", byte_count, limits,
            :policy_metadata_bytes; cancel_check)
        interval_semantics = _rou_bounded_string(
            policy.interval_semantics, "interval_semantics", byte_count,
            limits, :policy_metadata_bytes; cancel_check)
        interval_semantics in ("none", "unknown") && throw(ArgumentError(
            "interval_semantics is invalid"))
        coordinate_limit = uncertainty_class == :parametric ?
            limits.max_parameters : limits.max_observations
        coordinate_count = _rou_preflight_sized_collection(
            policy.coordinate_ids, "coordinate_ids",
            :replicate_coordinate_dimensions, coordinate_limit)
        coordinate_count > 0 || throw(ArgumentError(
            "interval policy requires at least one coordinate"))
        _rou_preflight_sized_collection(
            policy.coordinate_units, "coordinate_units",
            :replicate_coordinate_dimensions, coordinate_limit) ==
            coordinate_count || throw(DimensionMismatch(
                "interval coordinate metadata dimensions are invalid"))
        _rou_preflight_sized_collection(
            policy.coordinate_lower, "coordinate_lower",
            :replicate_coordinate_dimensions, coordinate_limit) ==
            coordinate_count || throw(DimensionMismatch(
                "interval coordinate metadata dimensions are invalid"))
        _rou_preflight_sized_collection(
            policy.coordinate_upper, "coordinate_upper",
            :replicate_coordinate_dimensions, coordinate_limit) ==
            coordinate_count || throw(DimensionMismatch(
                "interval coordinate metadata dimensions are invalid"))
        _rou_charge_metadata_bytes!(
            byte_count, 16 * BigInt(coordinate_count), limits,
            :policy_metadata_bytes)
        certificate_hash = _rou_bounded_string(
            policy.interval_certificate_sha256,
            "interval_certificate_sha256", byte_count, limits,
            :policy_metadata_bytes; cancel_check)
        definition_hash = _rou_bounded_string(
            policy.interval_definition_sha256,
            "interval_definition_sha256", byte_count, limits,
            :policy_metadata_bytes; cancel_check)
        coordinate_ids = _rou_bounded_string_tuple(
            policy.coordinate_ids, coordinate_count, "coordinate_ids",
            byte_count, limits, :policy_metadata_bytes;
            unique_names=true, cancel_check)
        coordinate_units = _rou_bounded_string_tuple(
            policy.coordinate_units, coordinate_count, "coordinate_units",
            byte_count, limits, :policy_metadata_bytes; cancel_check)
        coordinate_lower = _rou_bounded_finite_float_tuple(
            policy.coordinate_lower, coordinate_count, "coordinate_lower";
            cancel_check)
        coordinate_upper = _rou_bounded_finite_float_tuple(
            policy.coordinate_upper, coordinate_count, "coordinate_upper";
            cancel_check)
        isequal(policy.coordinate_lower, coordinate_lower) &&
            isequal(policy.coordinate_upper, coordinate_upper) ||
            throw(ArgumentError(
                "interval coordinate bounds are noncanonical"))
        all(coordinate_lower[index] <= coordinate_upper[index]
            for index in eachindex(coordinate_lower)) || throw(ArgumentError(
                "interval coordinate bounds are invalid"))
        _rou_sha256_string(certificate_hash,
            "interval_certificate_sha256") ==
            certificate_hash || throw(ArgumentError(
                "interval certificate hash is noncanonical"))
        names = _rou_canonical_named_tuple_names(
            policy.interval_specification; limits, cancel_check)
        _rou_reject_reserved_policy_names(
            names,
            (
                :uncertainty_class, :policy_id, :policy_revision,
                :population_count, :draw_count, :distribution_family,
                :sampling_revision, :enumeration, :interval_semantics,
                :coordinate_ids, :coordinate_units, :coordinate_lower,
                :coordinate_upper, :lower, :upper,
                :interval_definition_sha256, :interval_certificate_sha256,
            ),
            "interval_specification",
        )
        expected_definition_hash = _rou_sha256(
            _rou_interval_definition_payload(
                policy.population_count,
                :explicit_complete_coordinate_population,
                interval_semantics,
                coordinate_ids,
                coordinate_units,
                coordinate_lower,
                coordinate_upper,
                policy.interval_specification,
            ))
        _rou_sha256_string(definition_hash, "interval_definition_sha256") ==
            expected_definition_hash ||
            throw(ArgumentError(
                "interval definition hash does not match typed content"))
        return (
            kind=:certified_interval,
            uncertainty_class,
            policy_id,
            policy_revision,
            population_count=policy.population_count,
            enumeration=:explicit_complete_coordinate_population,
            coordinate_ids,
            coordinate_units,
            coordinate_lower,
            coordinate_upper,
            interval_definition_sha256=definition_hash,
            interval_certificate_sha256=certificate_hash,
        )
    end
    throw(ArgumentError("unsupported population policy payload type"))
end

"""Revalidate a local-identifiability artifact and every mutable snapshot."""
function validate_ro_local_identifiability_analysis(
    result::ROLocalIdentifiabilityAnalysis,
    ;
    cancel_check=()->nothing,
    limits::ROUncertaintyLimits=ROUncertaintyLimits(),
)
    _rou_cancel(cancel_check)
    limits = _rou_rebuild_limits(limits)
    _rou_require_exact_keys(
        result.identity_payload,
        (
            :schema_version, :coordinate_identity, :scientific_source,
            :scientific_source_sha256, :observation_validity,
            :observation_gap_reasons, :sensitivity_rows,
            :observation_model, :structural_ambiguity_reasons,
            :numerical_rank_policy, :evidence_scope, :result_snapshot,
        ),
        "local-identifiability payload",
    )
    payload, identity, source = _rou_validate_identity_envelope(
        result, RO_LOCAL_IDENTIFIABILITY_VERSION; limits, cancel_check)
    observation_count = length(identity.output_order)
    parameter_count = length(identity.parameter_order)
    length(source.local_coordinates) == length(identity.input_order) ||
        throw(DimensionMismatch("source coordinates do not match identity"))
    length(source.nominal_parameters) == parameter_count ||
        throw(DimensionMismatch("source parameters do not match identity"))
    length(source.observation_schedule) == observation_count ||
        throw(DimensionMismatch("source schedule does not match identity"))
    validity = _rou_payload_validity(
        payload.observation_validity,
        observation_count,
        "observation_validity",
    )
    valid_count = count(validity)
    covariance_model = payload.observation_model isa NamedTuple &&
        hasproperty(payload.observation_model, :kind) &&
        payload.observation_model.kind == :observation_covariance
    element_work = BigInt(observation_count) * parameter_count +
        3 * BigInt(parameter_count)^2 +
        (covariance_model ? BigInt(observation_count)^2 : BigInt(0))
    _rou_limit(:matrix_elements, element_work, limits.max_matrix_elements)
    factorization_work = BigInt(valid_count) * parameter_count *
        min(valid_count, parameter_count) + BigInt(parameter_count)^3 +
        (covariance_model ?
            BigInt(observation_count)^3 + BigInt(valid_count)^3 : BigInt(0))
    _rou_limit(
        :factorization_work,
        factorization_work,
        limits.max_factorization_work,
    )
    gap_reasons = _rou_gap_reasons(
        validity, payload.observation_gap_reasons,
        "observation_gap_reasons")
    reasons = _rou_structural_reasons(
        payload.structural_ambiguity_reasons; limits, cancel_check)
    sensitivity = _rou_payload_valid_rows(
        payload.sensitivity_rows,
        validity,
        parameter_count,
        "sensitivity_rows",
    )
    valid_indices = findall(validity)
    valid_sensitivity = sensitivity[valid_indices, :]
    model = payload.observation_model
    model isa NamedTuple && hasproperty(model, :kind) || throw(ArgumentError(
        "observation_model must be a typed NamedTuple"))
    whitened = Matrix{Float64}(undef, 0, parameter_count)
    experimental_status = :not_supplied
    if model.kind == :diagonal_precision_weights
        keys(model) == (:kind, :values, :valid_indices) ||
            throw(ArgumentError("precision-weight model fields are invalid"))
        model.values isa Tuple && length(model.values) == observation_count ||
            throw(DimensionMismatch("precision weights do not match outputs"))
        weights = Vector{Float64}(undef, observation_count)
        for index in 1:observation_count
            model.values[index] isa Float64 || throw(ArgumentError(
                "precision weights must use Float64"))
            weights[index] = model.values[index]
        end
        all(value -> isfinite(value) && value > 0, weights) ||
            throw(ArgumentError("precision weights are invalid"))
        model.valid_indices == Tuple(valid_indices) || throw(ArgumentError(
            "precision-weight valid indices are invalid"))
        whitened = valid_sensitivity .* sqrt.(weights[valid_indices])
        experimental_status = :observation_precision_weights_supplied
    elseif model.kind == :observation_covariance
        keys(model) == (
            :kind, :supplied_values, :values, :valid_indices,
            :valid_submatrix, :admission_policy,
        ) || throw(ArgumentError("covariance model fields are invalid"))
        admission = model.admission_policy
        _rou_require_exact_keys(admission, (
            :symmetry_relative_tolerance, :symmetry_residual,
            :symmetry_admission, :covariance_scale,
            :psd_relative_tolerance, :positive_definite_relative_floor,
            :positive_definite_effective_floor,
        ), "covariance admission policy")
        symmetry_tolerance = _rou_relative_tolerance(
            admission.symmetry_relative_tolerance,
            "symmetry_relative_tolerance",
        )
        symmetry_tolerance <= RO_COVARIANCE_MAX_SYMMETRY_RELATIVE_TOLERANCE ||
            throw(ArgumentError("symmetry tolerance exceeds safety ceiling"))
        psd_tolerance = _rou_relative_tolerance(
            admission.psd_relative_tolerance,
            "psd_relative_tolerance",
        )
        pd_floor = _rou_relative_tolerance(
            admission.positive_definite_relative_floor,
            "positive_definite_relative_floor",
        )
        pd_floor > 0 || throw(ArgumentError(
            "positive-definite floor must be positive"))
        supplied = _rou_payload_matrix(
            model.supplied_values,
            observation_count,
            observation_count,
            "supplied observation covariance",
        )
        covariance, residual, symmetry_admission, covariance_scale =
            _rou_symmetric_matrix(
                supplied, "observation_covariance", symmetry_tolerance)
        _rou_validate_numeric(
            _rou_payload_matrix(
                model.values,
                observation_count,
                observation_count,
                "canonical observation covariance",
            ),
            covariance,
            "canonical observation covariance",
        )
        _rou_validate_numeric(admission.symmetry_residual, residual,
            "covariance symmetry residual")
        _rou_validate_numeric(admission.symmetry_admission,
            symmetry_admission, "covariance symmetry admission")
        _rou_validate_numeric(admission.covariance_scale, covariance_scale,
            "covariance scale")
        _rou_psd_eigen(
            covariance,
            "observation_covariance",
            psd_tolerance;
            cancel_check=()->nothing,
        )
        model.valid_indices == Tuple(valid_indices) || throw(ArgumentError(
            "covariance valid indices are invalid"))
        valid_covariance = covariance[valid_indices, valid_indices]
        _rou_validate_numeric(
            _rou_payload_matrix(
                model.valid_submatrix,
                length(valid_indices),
                length(valid_indices),
                "valid observation covariance",
            ),
            valid_covariance,
            "valid observation covariance",
        )
        effective_pd_floor = nothing
        if !isempty(valid_indices)
            decomposition, scale, backward_floor, _ = _rou_psd_eigen(
                valid_covariance,
                "valid observation_covariance submatrix",
                psd_tolerance;
                cancel_check=()->nothing,
            )
            effective_pd_floor = max(pd_floor * scale, backward_floor)
            minimum(decomposition.values) > effective_pd_floor ||
                throw(ArgumentError(
                    "valid observation covariance is not safely positive definite"))
            factor = cholesky(Symmetric(valid_covariance))
            whitened = factor.L \ valid_sensitivity
        end
        _rou_validate_numeric(
            admission.positive_definite_effective_floor,
            effective_pd_floor,
            "positive-definite effective floor",
        )
        experimental_status = :observation_covariance_supplied
    else
        throw(ArgumentError("unsupported observation_model kind"))
    end
    all(isfinite, whitened) || throw(ArgumentError(
        "recomputed whitened sensitivity is non-finite"))

    rank_policy = payload.numerical_rank_policy
    _rou_require_exact_keys(rank_policy, (
        :method, :absolute_low, :absolute_high, :relative_low, :relative_high,
        :requested_low, :requested_high, :effective_low, :effective_high,
        :backward_error_floor,
        :backward_error_multiplier, :backward_error_floor_dominated,
    ), "numerical_rank_policy")
    rank_policy.method == :whitened_sensitivity_svd || throw(ArgumentError(
        "numerical-rank method must be whitened_sensitivity_svd"))
    absolute_low = _rou_canonical_float(rank_policy.absolute_low)
    absolute_high = _rou_canonical_float(rank_policy.absolute_high)
    relative_low = _rou_canonical_float(rank_policy.relative_low)
    relative_high = _rou_canonical_float(rank_policy.relative_high)
    0 <= absolute_low < absolute_high &&
        0 <= relative_low < relative_high &&
        all(isfinite,
            (absolute_low, absolute_high, relative_low, relative_high)) ||
        throw(ArgumentError("numerical-rank threshold inputs are invalid"))
    multiplier = _rou_canonical_float(rank_policy.backward_error_multiplier)
    isfinite(multiplier) && multiplier >= 1 || throw(ArgumentError(
        "backward-error multiplier is invalid"))

    common_status = _rou_common_status(validity, reasons)
    expected_partition = _rou_partition(
        validity,
        gap_reasons,
        reasons;
        parametric=:not_supplied,
        experimental=experimental_status,
    )
    _rou_validate_partition(
        result.uncertainty, expected_partition, "local-identifiability")
    _rou_validate(result.status == common_status,
        "local-identifiability status is invalid")

    fim = nothing
    singular_values = Float64[]
    fim_spectrum = Float64[]
    rank_lower = 0
    rank_upper = parameter_count
    numerical_rank = nothing
    rank_status = :no_valid_observations
    condition_number = nothing
    condition_status = :unavailable_no_valid_observations
    structural_status = :unknown_numerical_gap
    practical_status = :unknown_numerical_gap
    practical_covariance = nothing
    practical_standard_errors = nothing
    singular_scale = 0.0
    decomposition = nothing
    if !isempty(valid_indices)
        decomposition = svd(whitened; full=false)
        singular_values = Vector{Float64}(decomposition.S)
        if length(singular_values) < parameter_count
            append!(singular_values,
                zeros(Float64, parameter_count - length(singular_values)))
        end
        singular_scale = first(singular_values)
    end
    requested_low, requested_high, effective_low, effective_high,
        backward_floor, floor_dominated =
        _rou_rank_thresholds(
            singular_scale,
            length(valid_indices),
            parameter_count,
            absolute_low,
            absolute_high,
            relative_low,
            relative_high,
            multiplier,
        )
    if !isempty(valid_indices)
        rank_lower = count(>(effective_high), singular_values)
        rank_upper = count(>(effective_low), singular_values)
        rank_status = rank_lower == parameter_count ? :full_rank :
            rank_upper < parameter_count ? :rank_deficient :
            :ambiguous_threshold
        numerical_rank = rank_lower == rank_upper ? rank_lower : nothing
        fim = Matrix(Symmetric(transpose(whitened) * whitened))
        fim_spectrum = singular_values .^ 2
        condition_status = rank_status == :rank_deficient ?
            :unavailable_rank_deficient : :unavailable_ambiguous_threshold
        if rank_status == :full_rank
            ratio = first(singular_values) / last(singular_values)
            condition_number = ratio * ratio
            condition_status = isfinite(condition_number) ? :finite : :overflow
        end
        structural_status = if common_status == :unknown_multiple_evidence_gaps
            :unknown_multiple_evidence_gaps
        elseif common_status == :unknown_numerical_gap
            :unknown_numerical_gap
        elseif common_status == :unknown_structural_ambiguity
            :unknown_structural_ambiguity
        elseif rank_status == :full_rank
            :full_rank_local_sensitivity_under_declared_model
        elseif rank_status == :rank_deficient
            :rank_deficient_local_sensitivity_under_declared_model
        else
            :ambiguous_numerical_rank
        end
        practical_status = common_status == :unknown_numerical_gap ?
            :unknown_numerical_gap :
            common_status == :unknown_structural_ambiguity ?
                :unknown_structural_ambiguity :
            common_status == :unknown_multiple_evidence_gaps ?
                :unknown_multiple_evidence_gaps :
            rank_status == :full_rank ?
                :local_fim_linearized_precision_available :
            rank_status == :rank_deficient ? :unavailable_rank_deficient :
            :unavailable_ambiguous_rank
        if practical_status == :local_fim_linearized_precision_available
            vectors = Matrix(transpose(decomposition.Vt))
            practical_covariance = Matrix(Symmetric(
                vectors * Diagonal(inv.(singular_values .^ 2)) *
                transpose(vectors),
            ))
            practical_standard_errors = sqrt.(diag(practical_covariance))
        end
    end

    _rou_validate_numeric(rank_policy.requested_low, requested_low,
        "requested rank low threshold")
    _rou_validate_numeric(rank_policy.requested_high, requested_high,
        "requested rank high threshold")
    _rou_validate_numeric(rank_policy.effective_low, effective_low,
        "effective rank low threshold")
    _rou_validate_numeric(rank_policy.effective_high, effective_high,
        "effective rank high threshold")
    _rou_validate_numeric(rank_policy.backward_error_floor, backward_floor,
        "rank backward-error floor")
    _rou_validate(rank_policy.backward_error_floor_dominated == floor_dominated,
        "rank floor-dominated flag is invalid")
    _rou_validate_numeric(result.fim, fim, "Fisher information")
    _rou_validate_numeric(result.fim_singular_values, fim_spectrum,
        "Fisher singular values")
    _rou_validate_numeric(result.whitened_sensitivity_singular_values,
        singular_values, "whitened sensitivity singular values")
    _rou_validate(result.rank_lower_bound == rank_lower &&
        result.rank_upper_bound == rank_upper &&
        result.numerical_rank == numerical_rank &&
        result.rank_status == rank_status,
        "local numerical-rank result is invalid")
    _rou_validate_numeric(result.requested_rank_threshold_low, requested_low,
        "requested rank low threshold")
    _rou_validate_numeric(result.requested_rank_threshold_high, requested_high,
        "requested rank high threshold")
    _rou_validate_numeric(result.rank_threshold_low, effective_low,
        "effective rank low threshold")
    _rou_validate_numeric(result.rank_threshold_high, effective_high,
        "effective rank high threshold")
    _rou_validate_numeric(result.backward_error_floor, backward_floor,
        "rank backward-error floor")
    _rou_validate_numeric(result.backward_error_multiplier, multiplier,
        "rank backward-error multiplier")
    _rou_validate(result.rank_threshold_floor_dominated == floor_dominated &&
        result.rank_method == :whitened_sensitivity_svd,
        "rank policy result fields are invalid")
    _rou_validate_numeric(result.condition_number, condition_number,
        "condition number")
    _rou_validate(result.condition_status == condition_status &&
        result.structural_local_identifiability == structural_status &&
        result.practical_precision_status == practical_status,
        "local result status fields are invalid")
    _rou_validate_numeric(result.practical_parameter_covariance,
        practical_covariance, "practical parameter covariance")
    _rou_validate_numeric(result.practical_parameter_standard_errors,
        practical_standard_errors, "practical parameter standard errors")

    snapshot = _rou_local_result_snapshot(
        status=result.status,
        fim=result.fim,
        fim_singular_values=result.fim_singular_values,
        whitened_singular_values=
            result.whitened_sensitivity_singular_values,
        rank_lower_bound=result.rank_lower_bound,
        rank_upper_bound=result.rank_upper_bound,
        numerical_rank=result.numerical_rank,
        rank_status=result.rank_status,
        condition_number=result.condition_number,
        condition_status=result.condition_status,
        structural_status=result.structural_local_identifiability,
        practical_status=result.practical_precision_status,
        practical_covariance=result.practical_parameter_covariance,
        practical_standard_errors=result.practical_parameter_standard_errors,
    )
    _rou_require_exact_keys(
        payload.result_snapshot,
        keys(snapshot),
        "local-identifiability result_snapshot",
    )
    _rou_validate(isequal(payload.result_snapshot, snapshot),
        "local-identifiability result snapshot mismatch")
    _rou_validate_canonical_payload_floats(
        payload, "local-identifiability payload"; cancel_check)
    _rou_validate(result.identity_sha256 == _rou_sha256(payload),
        "local-identifiability artifact hash mismatch")
    _rou_validate(payload.evidence_scope == RO_LOCAL_IDENTIFIABILITY_SCOPE &&
        result.evidence_scope == RO_LOCAL_IDENTIFIABILITY_SCOPE &&
        !result.global_identifiability_claimed && !result.causal_claimed &&
        !result.experimentally_validated,
        "local-identifiability scope flags are invalid")
    return true
end

"""Revalidate a delta-method artifact and its factor/covariance snapshots."""
function validate_ro_delta_method_covariance(
    result::RODeltaMethodCovariance;
    cancel_check=()->nothing,
    limits::ROUncertaintyLimits=ROUncertaintyLimits(),
)
    _rou_cancel(cancel_check)
    limits = _rou_rebuild_limits(limits)
    _rou_require_exact_keys(
        result.identity_payload,
        (
            :schema_version, :coordinate_identity, :scientific_source,
            :scientific_source_sha256, :output_validity,
            :output_gap_reasons, :jacobian_rows, :parameter_covariance,
            :supplied_parameter_covariance,
            :structural_ambiguity_reasons, :covariance_admission_policy,
            :propagation, :method_scope, :result_snapshot,
        ),
        "delta-method payload",
    )
    payload, identity, source = _rou_validate_identity_envelope(
        result, RO_DELTA_METHOD_UNCERTAINTY_VERSION; limits, cancel_check)
    output_count = length(identity.output_order)
    parameter_count = length(identity.parameter_order)
    length(source.local_coordinates) == length(identity.input_order) ||
        throw(DimensionMismatch("source coordinates do not match identity"))
    length(source.nominal_parameters) == parameter_count ||
        throw(DimensionMismatch("source parameters do not match identity"))
    length(source.observation_schedule) == output_count ||
        throw(DimensionMismatch("source schedule does not match identity"))
    validity = _rou_payload_validity(
        payload.output_validity, output_count, "output_validity")
    valid_count = count(validity)
    element_work = BigInt(output_count) * parameter_count +
        2 * BigInt(parameter_count)^2 + 3 * BigInt(output_count)^2 +
        BigInt(output_count) * parameter_count
    _rou_limit(:matrix_elements, element_work, limits.max_matrix_elements)
    factorization_work = BigInt(parameter_count)^3 + BigInt(valid_count)^3 +
        BigInt(valid_count) * BigInt(parameter_count)^2 +
        BigInt(valid_count)^2 * parameter_count
    _rou_limit(
        :factorization_work,
        factorization_work,
        limits.max_factorization_work,
    )
    gap_reasons = _rou_gap_reasons(
        validity, payload.output_gap_reasons, "output_gap_reasons")
    reasons = _rou_structural_reasons(
        payload.structural_ambiguity_reasons; limits, cancel_check)
    jacobian = _rou_payload_valid_rows(
        payload.jacobian_rows, validity, parameter_count, "jacobian_rows")
    supplied_covariance = _rou_payload_matrix(
        payload.supplied_parameter_covariance,
        parameter_count,
        parameter_count,
        "supplied parameter covariance",
    )
    admission = payload.covariance_admission_policy
    _rou_require_exact_keys(admission, (
        :symmetry_relative_tolerance, :symmetry_residual,
        :symmetry_admission, :covariance_scale, :psd_relative_tolerance,
        :eigen_scale, :backward_error_floor, :ambiguity_floor,
    ), "delta covariance admission policy")
    symmetry_tolerance = _rou_relative_tolerance(
        admission.symmetry_relative_tolerance,
        "symmetry_relative_tolerance",
    )
    symmetry_tolerance <= RO_COVARIANCE_MAX_SYMMETRY_RELATIVE_TOLERANCE ||
        throw(ArgumentError("symmetry tolerance exceeds safety ceiling"))
    psd_tolerance = _rou_relative_tolerance(
        admission.psd_relative_tolerance, "psd_relative_tolerance")
    parameter_covariance, residual, symmetry_admission, covariance_scale =
        _rou_symmetric_matrix(
            supplied_covariance,
            "parameter_covariance",
            symmetry_tolerance,
        )
    _rou_validate_numeric(
        _rou_payload_matrix(
            payload.parameter_covariance,
            parameter_count,
            parameter_count,
            "canonical parameter covariance",
        ),
        parameter_covariance,
        "canonical parameter covariance",
    )
    _rou_validate_numeric(admission.symmetry_residual, residual,
        "parameter covariance symmetry residual")
    _rou_validate_numeric(admission.symmetry_admission, symmetry_admission,
        "parameter covariance symmetry admission")
    _rou_validate_numeric(admission.covariance_scale, covariance_scale,
        "parameter covariance scale")
    decomposition, eigen_scale, backward_floor, ambiguity_floor =
        _rou_psd_eigen(
            parameter_covariance,
            "parameter_covariance",
            psd_tolerance;
            cancel_check=()->nothing,
        )
    _rou_validate_numeric(admission.eigen_scale, eigen_scale,
        "parameter covariance eigen scale")
    _rou_validate_numeric(admission.backward_error_floor, backward_floor,
        "parameter covariance backward-error floor")
    _rou_validate_numeric(admission.ambiguity_floor, ambiguity_floor,
        "parameter covariance ambiguity floor")

    valid_indices = findall(validity)
    recomputed_parameter_factor = decomposition.vectors *
        Diagonal(sqrt.(decomposition.values))
    expected_covariance = Matrix{Float64}(undef, 0, 0)
    expected_output_factor = Matrix{Float64}(undef, 0, parameter_count)
    expected_standard_deviations = fill(NaN, output_count)
    output_psd_status = :unavailable_no_valid_outputs
    output_minimum = nothing
    output_backward_floor = nothing
    if !isempty(valid_indices)
        valid_jacobian = jacobian[valid_indices, :]
        expected_output_factor = valid_jacobian *
            recomputed_parameter_factor
        expected_covariance = Matrix(Symmetric(
            expected_output_factor * transpose(expected_output_factor),
        ))
        spectrum = eigvals(Symmetric(expected_covariance))
        output_scale = max(
            _rou_matrix_scale(expected_covariance),
            maximum(abs, spectrum; init=0.0),
        )
        output_backward_floor = 32 * eps(Float64) *
            max(1, length(valid_indices)) * output_scale
        output_minimum = minimum(spectrum)
        output_minimum < -output_backward_floor && throw(ArgumentError(
            "recomputed delta covariance violates its PSD backward bound"))
        output_psd_status = output_minimum < 0 ?
            :factor_propagated_roundoff_grey_zone : :factor_propagated_psd
        for (local_index, output_index) in enumerate(valid_indices)
            expected_standard_deviations[output_index] =
                norm(@view expected_output_factor[local_index, :])
        end
    end

    expected_partition = _rou_partition(
        validity,
        gap_reasons,
        reasons;
        parametric=:parameter_covariance_supplied,
        experimental=:not_supplied,
    )
    _rou_validate_partition(result.uncertainty, expected_partition, "delta")
    expected_status = _rou_common_status(validity, reasons)
    _rou_validate(result.status == expected_status,
        "delta-method status is invalid")
    size(result.parameter_covariance_factor) ==
        (parameter_count, parameter_count) || throw(DimensionMismatch(
            "parameter covariance factor has invalid dimensions"))
    all(isfinite, result.parameter_covariance_factor) || throw(ArgumentError(
        "parameter covariance factor must be finite"))
    reconstructed_parameter_covariance =
        result.parameter_covariance_factor *
        transpose(result.parameter_covariance_factor)
    _rou_validate_absolute_bound(
        reconstructed_parameter_covariance,
        parameter_covariance,
        max(backward_floor, _rou_matmul_backward_bound(
            result.parameter_covariance_factor,
            transpose(result.parameter_covariance_factor),
        )),
        "parameter covariance factor",
    )
    size(result.output_covariance_factor) ==
        (output_count, parameter_count) || throw(DimensionMismatch(
            "output covariance factor has invalid dimensions"))
    size(result.output_covariance) == (output_count, output_count) ||
        throw(DimensionMismatch("output covariance has invalid dimensions"))
    length(result.output_standard_deviations) == output_count ||
        throw(DimensionMismatch(
            "output standard deviations have invalid dimensions"))
    if !isempty(valid_indices)
        valid_output_factor =
            result.output_covariance_factor[valid_indices, :]
        stored_factor_pushforward = jacobian[valid_indices, :] *
            result.parameter_covariance_factor
        _rou_validate_absolute_bound(
            valid_output_factor,
            stored_factor_pushforward,
            _rou_matmul_backward_bound(
                jacobian[valid_indices, :],
                result.parameter_covariance_factor,
            ),
            "delta output covariance factor",
        )
        stored_covariance =
            result.output_covariance[valid_indices, valid_indices]
        factor_covariance = valid_output_factor * transpose(valid_output_factor)
        factor_covariance_bound = _rou_matmul_backward_bound(
            valid_output_factor, transpose(valid_output_factor))
        _rou_validate_absolute_bound(
            stored_covariance,
            factor_covariance,
            factor_covariance_bound,
            "stored delta covariance/factor consistency",
        )
        recomputed_covariance_bound = factor_covariance_bound +
            _rou_matmul_backward_bound(
                expected_output_factor, transpose(expected_output_factor))
        _rou_validate_absolute_bound(
            stored_covariance,
            expected_covariance,
            recomputed_covariance_bound,
            "delta output covariance",
        )
        for (local_index, output_index) in enumerate(valid_indices)
            stored_standard_deviation =
                result.output_standard_deviations[output_index]
            factor_standard_deviation =
                norm(@view valid_output_factor[local_index, :])
            standard_deviation_bound = 128 * eps(Float64) *
                max(1, parameter_count) *
                max(factor_standard_deviation, floatmin(Float64))
            abs(stored_standard_deviation - factor_standard_deviation) <=
                standard_deviation_bound || throw(ArgumentError(
                    "delta output standard deviation is inconsistent with " *
                    "its stored PSD factor"))
            abs(stored_standard_deviation -
                expected_standard_deviations[output_index]) <=
                2 * standard_deviation_bound || throw(ArgumentError(
                    "delta output standard deviation does not match the " *
                    "recomputed eigen-factor pushforward"))
        end
    end
    _rou_validate(result.valid_output_count == length(valid_indices) &&
        result.invalid_output_count == output_count - length(valid_indices),
        "delta valid/invalid counts are invalid")
    _rou_validate_numeric(result.covariance_psd_tolerance, psd_tolerance,
        "delta PSD tolerance")
    _rou_validate(result.output_psd_status == output_psd_status,
        "delta output PSD status is invalid")
    _rou_validate_numeric(result.output_covariance_minimum_eigenvalue,
        output_minimum, "delta minimum eigenvalue")
    _rou_validate_numeric(result.output_covariance_backward_error_floor,
        output_backward_floor, "delta output backward-error floor")
    propagation = payload.propagation
    _rou_require_exact_keys(propagation, (
        :method, :output_psd_status, :output_minimum_eigenvalue,
        :output_backward_error_floor, :variance_clipping_performed,
    ), "delta propagation payload")
    _rou_validate(propagation.method == :psd_factor_pushforward &&
        propagation.output_psd_status == output_psd_status &&
        propagation.variance_clipping_performed === false,
        "delta propagation payload is invalid")
    _rou_validate_numeric(propagation.output_minimum_eigenvalue,
        output_minimum, "payload delta minimum eigenvalue")
    _rou_validate_numeric(propagation.output_backward_error_floor,
        output_backward_floor, "payload delta backward-error floor")
    _rou_validate(payload.method_scope ==
        :first_order_local_delta_method_only,
        "delta payload method scope is invalid")
    snapshot = _rou_delta_result_snapshot(
        status=result.status,
        validity=validity,
        parameter_factor=result.parameter_covariance_factor,
        output_factor=result.output_covariance_factor,
        output_covariance=result.output_covariance,
        output_standard_deviations=result.output_standard_deviations,
        output_psd_status=result.output_psd_status,
        output_minimum=result.output_covariance_minimum_eigenvalue,
        output_backward_floor=
            result.output_covariance_backward_error_floor,
    )
    _rou_require_exact_keys(
        payload.result_snapshot,
        keys(snapshot),
        "delta-method result_snapshot",
    )
    _rou_validate(isequal(payload.result_snapshot, snapshot),
        "delta-method result snapshot mismatch")
    invalid_indices = findall(.!validity)
    for index in invalid_indices
        _rou_validate(all(isnan, @view result.output_covariance_factor[index, :]) &&
            all(isnan, @view result.output_covariance[index, :]) &&
            all(isnan, @view result.output_covariance[:, index]) &&
            isnan(result.output_standard_deviations[index]),
            "delta-method invalid outputs must remain explicit NaN gaps")
    end
    _rou_validate_canonical_payload_floats(
        payload, "delta-method payload"; cancel_check)
    _rou_validate(result.identity_sha256 == _rou_sha256(payload),
        "delta-method artifact hash mismatch")
    _rou_validate(result.propagation_method == :psd_factor_pushforward &&
        result.method_scope == :first_order_local_delta_method_only &&
        result.calibration_status == :not_assessed && !result.causal_claimed &&
        !result.experimentally_validated,
        "delta-method scope flags are invalid")
    return true
end

"""Revalidate synthetic coverage counts, wording, feature identity, and hash."""
function validate_ro_synthetic_coverage_evidence(
    result::ROSyntheticCoverageEvidence,
    ;
    cancel_check=()->nothing,
    limits::ROUncertaintyLimits=ROUncertaintyLimits(),
)
    _rou_cancel(cancel_check)
    limits = _rou_rebuild_limits(limits)
    payload = result.identity_payload
    _rou_require_exact_keys(
        payload,
        (
            :schema_version, :fixture_id, :source_fixture_sha256,
            :feature_ids, :expected_case_count, :case_ids, :case_validity,
            :case_gap_reasons, :truth_rows, :lower_rows, :upper_rows,
            :coverage_semantics, :calibration_scope, :result_snapshot,
        ),
        "synthetic-coverage payload",
    )
    _rou_validate(result.schema_version == RO_SYNTHETIC_COVERAGE_VERSION &&
        payload.schema_version == RO_SYNTHETIC_COVERAGE_VERSION,
        "synthetic-coverage schema version mismatch")
    payload.expected_case_count isa Int &&
        payload.expected_case_count > 0 || throw(ArgumentError(
            "synthetic expected_case_count is invalid"))
    case_count = payload.expected_case_count
    case_count <= limits.max_calibration_cases ||
        throw(ROUncertaintyLimitExceeded(
            :calibration_cases, BigInt(case_count),
            limits.max_calibration_cases))
    payload.feature_ids isa Tuple || throw(ArgumentError(
        "synthetic-coverage feature_ids must be a Tuple"))
    feature_count = length(payload.feature_ids)
    feature_count > 0 || throw(ArgumentError(
        "synthetic coverage requires at least one feature"))
    feature_count <= limits.max_outputs || throw(ROUncertaintyLimitExceeded(
        :calibration_features, BigInt(feature_count), limits.max_outputs))
    payload.case_ids isa Tuple || throw(ArgumentError(
        "synthetic case_ids must be a Tuple"))
    length(payload.case_ids) == case_count || throw(DimensionMismatch(
        "synthetic case_ids do not match expected_case_count"))
    _rou_limit(
        :matrix_elements,
        3 * BigInt(case_count) * feature_count,
        limits.max_matrix_elements,
    )
    metadata_bytes = Ref(BigInt(0))
    fixture = _rou_bounded_string(
        payload.fixture_id, "fixture_id", metadata_bytes, limits,
        :coverage_metadata_bytes; cancel_check)
    fixture == result.fixture_id || throw(ArgumentError(
        "synthetic-coverage fixture identity mismatch"))
    source_hash_text = _rou_bounded_string(
        payload.source_fixture_sha256, "source_fixture_sha256",
        metadata_bytes, limits, :coverage_metadata_bytes; cancel_check)
    fixture_sha = _rou_sha256_string(
        source_hash_text, "source_fixture_sha256")
    fixture_sha == result.source_fixture_sha256 || throw(ArgumentError(
        "synthetic-coverage fixture hash mismatch"))
    features = _rou_bounded_string_tuple(
        payload.feature_ids, feature_count, "feature_ids", metadata_bytes,
        limits, :coverage_metadata_bytes; unique_names=true, cancel_check)
    features == result.feature_ids || throw(ArgumentError(
        "synthetic-coverage feature identity mismatch"))
    ids = _rou_bounded_string_tuple(
        payload.case_ids, case_count, "case_ids", metadata_bytes, limits,
        :coverage_metadata_bytes; unique_names=true, cancel_check)
    validity = _rou_payload_validity(
        payload.case_validity, case_count, "case_validity")
    gap_reasons = _rou_gap_reasons(
        validity, payload.case_gap_reasons, "case_gap_reasons")
    truth = _rou_payload_valid_rows(
        payload.truth_rows, validity, feature_count, "truth_rows")
    lower = _rou_payload_valid_rows(
        payload.lower_rows, validity, feature_count, "lower_rows")
    upper = _rou_payload_valid_rows(
        payload.upper_rows, validity, feature_count, "upper_rows")
    _rou_validate(payload.coverage_semantics ==
        :truth_inside_closed_feature_interval &&
        payload.calibration_scope == :synthetic_fixture_only,
        "synthetic coverage scope or semantics are invalid")
    valid_indices = findall(validity)
    feature_counts = zeros(Int, feature_count)
    feature_valid_counts = fill(length(valid_indices), feature_count)
    joint_count = 0
    for row in valid_indices
        covered_jointly = true
        for feature in 1:feature_count
            lower[row, feature] <= upper[row, feature] || throw(ArgumentError(
                "synthetic lower bounds exceed upper bounds"))
            covered = lower[row, feature] <= truth[row, feature] <=
                upper[row, feature]
            feature_counts[feature] += covered
            covered_jointly &= covered
        end
        joint_count += covered_jointly
    end
    feature_coverage = Union{Nothing,Float64}[
        isempty(valid_indices) ? nothing :
            feature_counts[index] / length(valid_indices)
        for index in 1:feature_count
    ]
    joint_coverage = isempty(valid_indices) ? nothing :
        joint_count / length(valid_indices)
    invalid_indices = findall(.!validity)
    invalid_case_ids = Tuple(ids[index] for index in invalid_indices)
    invalid_reasons = Tuple(
        gap_reasons[index]::Symbol for index in invalid_indices)
    coverage_status = all(validity) ? :complete_synthetic_fixture :
        :unknown_incomplete_synthetic_fixture
    calibration_status = all(validity) ?
        :synthetic_coverage_evaluated_not_calibrated :
        :unknown_incomplete_synthetic_coverage_not_calibrated
    _rou_validate(result.expected_case_count == case_count &&
        result.valid_case_count == length(valid_indices) &&
        result.invalid_case_count == length(invalid_indices) &&
        result.invalid_case_ids == invalid_case_ids &&
        result.invalid_gap_reasons == invalid_reasons,
        "synthetic coverage case accounting is invalid")
    _rou_validate_numeric(result.feature_coverage_counts, feature_counts,
        "synthetic feature coverage counts")
    _rou_validate_numeric(result.feature_valid_counts, feature_valid_counts,
        "synthetic feature valid counts")
    _rou_validate_numeric(result.feature_coverage, feature_coverage,
        "synthetic feature coverage")
    _rou_validate(result.joint_coverage_count == joint_count &&
        result.joint_valid_count == length(valid_indices),
        "synthetic joint coverage counts are invalid")
    _rou_validate_numeric(result.joint_coverage, joint_coverage,
        "synthetic joint coverage")
    _rou_validate(result.status == coverage_status &&
        result.calibration_status == calibration_status,
        "synthetic coverage status is invalid")
    snapshot = _rou_coverage_result_snapshot(
        valid_case_count=result.valid_case_count,
        invalid_case_count=result.invalid_case_count,
        invalid_case_ids=result.invalid_case_ids,
        invalid_gap_reasons=result.invalid_gap_reasons,
        feature_coverage_counts=result.feature_coverage_counts,
        feature_valid_counts=result.feature_valid_counts,
        feature_coverage=result.feature_coverage,
        joint_coverage_count=result.joint_coverage_count,
        joint_valid_count=result.joint_valid_count,
        joint_coverage=result.joint_coverage,
        status=result.status,
        calibration_status=result.calibration_status,
    )
    _rou_require_exact_keys(
        payload.result_snapshot,
        keys(snapshot),
        "synthetic-coverage result_snapshot",
    )
    _rou_validate(isequal(payload.result_snapshot, snapshot),
        "synthetic-coverage result snapshot mismatch")
    _rou_validate_canonical_payload_floats(
        payload, "synthetic-coverage payload"; cancel_check)
    _rou_validate(result.identity_sha256 == _rou_sha256(payload),
        "synthetic-coverage artifact hash mismatch")
    _rou_validate(!result.experimentally_calibrated &&
        result.calibration_status in (
            :synthetic_coverage_evaluated_not_calibrated,
            :unknown_incomplete_synthetic_coverage_not_calibrated,
        ), "synthetic coverage cannot claim experimental calibration")
    return true
end

"""Revalidate a population artifact, mutable summaries, policy, and hash."""
function validate_ro_uncertainty_population_artifact(
    result::ROUncertaintyPopulationArtifact,
    ;
    cancel_check=()->nothing,
    limits::ROUncertaintyLimits=ROUncertaintyLimits(),
)
    _rou_cancel(cancel_check)
    limits = _rou_rebuild_limits(limits)
    _rou_require_exact_keys(
        result.identity_payload,
        (
            :schema_version, :coordinate_identity, :scientific_source,
            :scientific_source_sha256, :uncertainty_class, :policy,
            :expected_population_count, :population_ids,
            :replicate_coordinate_ids, :replicate_coordinates,
            :replicate_validity, :replicate_gap_reasons, :feature_rows,
            :quantile_probabilities, :quantile_scope, :certified_bounds,
            :structural_ambiguity_reasons,
            :calibration_evidence_sha256, :evidence_scope,
            :result_snapshot,
        ),
        "uncertainty-population payload",
    )
    payload, identity, source = _rou_validate_identity_envelope(
        result, RO_UNCERTAINTY_POPULATION_VERSION; limits, cancel_check)
    feature_count = length(identity.output_order)
    feature_count > 0 || throw(ArgumentError(
        "uncertainty population requires at least one feature"))
    length(source.local_coordinates) == length(identity.input_order) ||
        throw(DimensionMismatch("source coordinates do not match identity"))
    length(source.nominal_parameters) == length(identity.parameter_order) ||
        throw(DimensionMismatch("source parameters do not match identity"))
    length(source.observation_schedule) == feature_count ||
        throw(DimensionMismatch("source schedule does not match identity"))
    policy = _rou_validate_population_policy_payload(
        payload.policy; limits, cancel_check)
    payload.uncertainty_class == policy.uncertainty_class ||
        throw(ArgumentError(
            "top-level uncertainty_class contradicts typed policy"))
    replicate_count = policy.population_count
    payload.expected_population_count isa Int &&
        payload.expected_population_count == replicate_count ||
        throw(ArgumentError(
            "population count is not owned by its typed policy"))
    replicate_count <= limits.max_replicates ||
        throw(ROUncertaintyLimitExceeded(
            :replicate_population, BigInt(replicate_count),
            limits.max_replicates))
    payload.population_ids isa Tuple || throw(ArgumentError(
        "population_ids must be a Tuple"))
    length(payload.population_ids) == replicate_count || throw(DimensionMismatch(
        "population_ids do not contain the complete population"))
    payload.replicate_coordinate_ids isa Tuple || throw(ArgumentError(
        "replicate_coordinate_ids must be a Tuple"))
    coordinate_count = length(payload.replicate_coordinate_ids)
    coordinate_count > 0 || throw(ArgumentError(
        "population must have at least one replicate coordinate"))
    coordinate_limit = policy.uncertainty_class == :parametric ?
        limits.max_parameters : limits.max_observations
    coordinate_count <= coordinate_limit ||
        throw(ROUncertaintyLimitExceeded(
            :replicate_coordinate_dimensions,
            BigInt(coordinate_count),
            coordinate_limit,
        ))
    byte_count = Ref(BigInt(0))
    ids = _rou_bounded_string_tuple(
        payload.population_ids, replicate_count, "population_ids", byte_count,
        limits, :population_metadata_bytes; unique_names=true, cancel_check)
    coordinate_ids = _rou_bounded_string_tuple(
        payload.replicate_coordinate_ids, coordinate_count,
        "replicate_coordinate_ids", byte_count, limits,
        :population_metadata_bytes; unique_names=true, cancel_check)
    payload.quantile_probabilities isa Tuple || throw(ArgumentError(
        "quantile_probabilities must be a Tuple"))
    quantile_count = length(payload.quantile_probabilities)
    _rou_limit(:quantiles, BigInt(quantile_count), limits.max_quantiles)
    quantile_elements = BigInt(quantile_count) * feature_count
    _rou_limit(
        :quantile_matrix_elements,
        quantile_elements,
        limits.max_matrix_elements,
    )
    _rou_limit(
        :matrix_elements,
        BigInt(replicate_count) * (feature_count + coordinate_count) +
            2 * BigInt(feature_count)^2 + quantile_elements,
        limits.max_matrix_elements,
    )
    coordinates = _rou_payload_matrix(
        payload.replicate_coordinates,
        replicate_count,
        coordinate_count,
        "replicate_coordinates",
    )
    all(isfinite, coordinates) || throw(ArgumentError(
        "replicate coordinates must be finite"))
    validity = _rou_payload_validity(
        payload.replicate_validity,
        replicate_count,
        "replicate_validity",
    )
    valid_count = count(validity)
    quantile_work = policy.kind == :ensemble_distribution ?
        BigInt(feature_count) * _rou_log2_work(valid_count) *
            (BigInt(valid_count) + quantile_count) : BigInt(0)
    _rou_limit(:sort_work, quantile_work, limits.max_sort_work)
    gap_reasons = _rou_gap_reasons(
        validity, payload.replicate_gap_reasons,
        "replicate_gap_reasons")
    reasons = _rou_structural_reasons(
        payload.structural_ambiguity_reasons; limits, cancel_check)
    features = _rou_payload_valid_rows(
        payload.feature_rows, validity, feature_count, "feature_rows")
    valid_indices = findall(validity)
    invalid_indices = findall(.!validity)
    probabilities = _rou_quantile_probabilities(
        payload.quantile_probabilities)
    feature_quantiles = nothing
    certified_bounds = nothing
    quantile_scope = :not_applicable
    bounds_status = :not_applicable
    if policy.kind == :ensemble_distribution
        isempty(probabilities) && throw(ArgumentError(
            "ensemble policy requires quantile probabilities"))
        payload.certified_bounds === nothing || throw(ArgumentError(
            "ensemble policy cannot carry certified bounds"))
        if !isempty(valid_indices)
            feature_quantiles = Matrix{Float64}(
                undef, length(probabilities), feature_count)
            probability_values = collect(probabilities)
            for feature in 1:feature_count
                feature_quantiles[:, feature] .= quantile(
                    features[valid_indices, feature], probability_values)
            end
        end
        quantile_scope = :empirical_conditional_on_valid_replicates
        result.interval_certificate_sha256 === nothing ||
            throw(ArgumentError(
                "ensemble population cannot carry an interval certificate"))
    elseif policy.kind == :certified_interval
        coordinate_ids == policy.coordinate_ids || throw(DimensionMismatch(
            "artifact coordinate order contradicts typed interval policy"))
        coordinate_rows = Tuple(
            Tuple(coordinates[row, :]) for row in axes(coordinates, 1))
        allunique(coordinate_rows) || throw(ArgumentError(
            "explicit complete coordinate population contains duplicate rows"))
        for row in axes(coordinates, 1), coordinate in axes(coordinates, 2)
            policy.coordinate_lower[coordinate] <= coordinates[row, coordinate] <=
                policy.coordinate_upper[coordinate] || throw(ArgumentError(
                    "artifact coordinate lies outside typed interval bounds"))
        end
        isempty(probabilities) || throw(ArgumentError(
            "certified interval cannot carry quantile probabilities"))
        certified_bounds = _rou_payload_matrix(
            payload.certified_bounds,
            feature_count,
            2,
            "certified_bounds",
        )
        all(isfinite, certified_bounds) &&
            all(certified_bounds[:, 1] .<= certified_bounds[:, 2]) ||
            throw(ArgumentError("certified bounds are invalid"))
        for row in valid_indices, feature in 1:feature_count
            certified_bounds[feature, 1] <= features[row, feature] <=
                certified_bounds[feature, 2] || throw(ArgumentError(
                    "enumerated population contradicts certified bounds"))
        end
        result.interval_certificate_sha256 ==
            policy.interval_certificate_sha256 || throw(ArgumentError(
                "interval population certificate is invalid"))
        bounds_status = :declared_certificate_bound_to_policy_not_reproved
    else
        throw(ArgumentError("unsupported population policy kind"))
    end
    parametric_status = policy.uncertainty_class == :parametric ?
        (policy.kind == :ensemble_distribution ?
            :declared_distribution_population :
            :declared_interval_population) : :not_supplied
    experimental_status = policy.uncertainty_class == :experimental ?
        (policy.kind == :ensemble_distribution ?
            :declared_distribution_population :
            :declared_interval_population) : :not_supplied
    expected_partition = _rou_partition(
        validity,
        gap_reasons,
        reasons;
        parametric=parametric_status,
        experimental=experimental_status,
    )
    _rou_validate_partition(
        result.uncertainty, expected_partition, "uncertainty population")
    expected_status = _rou_common_status(validity, reasons)
    expected_gap_probability = length(invalid_indices) / replicate_count
    _rou_validate(result.status == expected_status &&
        result.uncertainty_class == policy.uncertainty_class &&
        result.policy_kind == policy.kind &&
        result.policy_id == policy.policy_id &&
        result.policy_revision == policy.policy_revision &&
        result.expected_population_count == replicate_count,
        "population policy/result metadata are invalid")
    _rou_validate(result.population_ids == ids &&
        result.replicate_coordinate_ids == coordinate_ids,
        "population identifiers are invalid")
    _rou_validate_numeric(result.replicate_coordinates, coordinates,
        "population replicate coordinates")
    _rou_validate(result.valid_replicate_count == length(valid_indices) &&
        result.invalid_replicate_count == length(invalid_indices),
        "population valid/invalid counts are invalid")
    _rou_validate_numeric(result.gap_probability, expected_gap_probability,
        "population gap probability")
    _rou_validate(result.quantile_probabilities == probabilities &&
        result.quantile_scope == quantile_scope &&
        payload.quantile_scope == quantile_scope,
        "population quantile policy is invalid")
    _rou_validate_numeric(result.feature_quantiles, feature_quantiles,
        "population feature quantiles")
    _rou_validate_numeric(result.certified_bounds, certified_bounds,
        "population certified bounds")
    _rou_validate(result.bounds_status == bounds_status,
        "population bounds status is invalid")
    calibration_status = :not_assessed
    if result.calibration_evidence === nothing
        _rou_validate(payload.calibration_evidence_sha256 === nothing,
            "uncertainty-population calibration identity mismatch")
    else
        validate_ro_synthetic_coverage_evidence(
            result.calibration_evidence; limits, cancel_check)
        result.calibration_evidence.feature_ids == identity.output_order ||
            throw(ArgumentError(
                "population calibration feature identity mismatch"))
        _rou_validate(payload.calibration_evidence_sha256 ==
            result.calibration_evidence.identity_sha256,
            "uncertainty-population calibration identity mismatch")
        calibration_status = result.calibration_evidence.calibration_status
    end
    _rou_validate(result.calibration_status == calibration_status,
        "population calibration status is invalid")
    snapshot = _rou_population_result_snapshot(
        status=result.status,
        uncertainty_class=result.uncertainty_class,
        policy_kind=result.policy_kind,
        expected_population_count=result.expected_population_count,
        valid_replicate_count=result.valid_replicate_count,
        invalid_replicate_count=result.invalid_replicate_count,
        gap_probability=result.gap_probability,
        feature_quantiles=result.feature_quantiles,
        bounds_status=result.bounds_status,
        calibration_status=result.calibration_status,
    )
    _rou_require_exact_keys(
        payload.result_snapshot,
        keys(snapshot),
        "uncertainty-population result_snapshot",
    )
    _rou_validate(isequal(payload.result_snapshot, snapshot),
        "uncertainty-population result snapshot mismatch")
    _rou_validate_canonical_payload_floats(
        payload, "uncertainty-population payload"; cancel_check)
    _rou_validate(result.identity_sha256 == _rou_sha256(payload),
        "uncertainty-population artifact hash mismatch")
    _rou_validate(payload.evidence_scope == :declared_population_only &&
        result.evidence_scope == :declared_population_only &&
        !result.causal_claimed && !result.global_robustness_claimed &&
        !result.experimentally_validated,
        "uncertainty-population scope flags are invalid")
    return true
end
